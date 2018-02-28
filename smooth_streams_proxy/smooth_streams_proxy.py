import base64
import binascii
import configparser
import gzip
import json
import logging.handlers
import logging.handlers
import os
import pprint
import random
import shelve
import socket
import sys
import threading
import urllib.parse
from datetime import datetime
from datetime import timedelta
from email.utils import formatdate
from enum import Enum
from http.server import BaseHTTPRequestHandler
from http.server import HTTPServer

import m3u8
import pytz
import requests
from cryptography.fernet import Fernet
from cryptography.fernet import InvalidToken
from lxml import etree
from tzlocal import get_localzone
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .exceptions import SmoothStreamsProxyException

TRACE = 5
VALID_LOGGING_LEVEL_VALUES = ['DEBUG', 'ERROR', 'INFO', 'TRACE']
VALID_SMOOTH_STREAMS_PROTOCOL_VALUES = ['hls', 'rtmp']
VALID_SMOOTH_STREAMS_SERVER_VALUES = ['dap', 'deu', 'deu-de', 'deu-nl', 'deu-nl1', 'deu-nl2', 'deu-nl3', 'deu-nl4',
                                      'deu-nl5', 'deu-uk', 'deu-uk1', 'deu-uk2', 'dna', 'dnae', 'dnae1', 'dnae2',
                                      'dnae3', 'dnae4', 'dnae6', 'dnaw', 'dnaw1', 'dnaw2', 'dnaw3', 'dnaw4x'
                                      ]
VALID_SMOOTH_STREAMS_SERVICE_VALUES = ['view247', 'viewmmasr', 'viewss', 'viewstvn']
VERSION = '1.2.0'

logger = logging.getLogger(__name__)


class MultiLineFormatter(logging.Formatter):
    def format(self, record):
        formatted_string = logging.Formatter.format(self, record)
        header, footer = formatted_string.split(record.message)
        formatted_string = formatted_string.replace('\n', '\n' + ' ' * len(header))
        return formatted_string


class PasswordState(Enum):
    DECRYPTED = 0
    ENCRYPTED = 1


class SmoothStreamsProxyHTTPRequestHandler(BaseHTTPRequestHandler):
    _lock = threading.Lock()

    @classmethod
    def _download_chunks_m3u8(cls, path, client_ip_address):
        try:
            full_url = '{0}{1}'.format(
                SmoothStreamsProxy.get_serviceable_client_parameter(client_ip_address,
                                                                    'last_requested_channel_url'),
                path)
        except KeyError:
            logger.error('Client {0} not in serviceable clients'.format(client_ip_address))

            return requests.codes.BAD_REQUEST, None

        logger.debug('Proxying request for {0} from {1} to {2}'.format(path, client_ip_address, full_url))

        smooth_streams_session = SmoothStreamsProxy.get_session_parameter('http_session')
        parsed_url = urllib.parse.urlparse(full_url)

        response = SmoothStreamsProxy.make_http_request(smooth_streams_session.get,
                                                        '{0}://{1}{2}'.format(parsed_url.scheme,
                                                                              parsed_url.netloc,
                                                                              parsed_url.path),
                                                        params=dict(urllib.parse.parse_qsl(parsed_url.query)),
                                                        headers=smooth_streams_session.headers,
                                                        cookies=smooth_streams_session.cookies.get_dict())

        response_status_code = response.status_code
        if response_status_code == requests.codes.ok:
            response_headers = response.headers
            response_text = response.text

            logger.trace(
                'Response from {0}:\n'
                '[Status Code]\n=============\n{1}\n\n'
                '[Header]\n========\n{2}\n\n'
                '[Content]\n=========\n{3}\n'.format(full_url,
                                                     response_status_code,
                                                     '\n'.join(['{0:32} => {1!s}'.format(
                                                         key,
                                                         response_headers[key]) for key in sorted(response_headers)]),
                                                     response_text))

            return response_status_code, response_text
        else:
            logger.error(
                'HTTP error {0} encountered requesting {1} for {2}'.format(response_status_code, full_url,
                                                                           client_ip_address))

            return response_status_code, None

    @classmethod
    def _download_playlist_m3u8(cls, path, client_ip_address, smooth_streams_hash):
        try:
            full_url = '{0}/playlist.m3u8?wmsAuthSign={1}'.format(
                SmoothStreamsProxy.get_serviceable_client_parameter(client_ip_address,
                                                                    'last_requested_channel_url'),
                smooth_streams_hash)
        except KeyError:
            logger.error('Client {0} not in serviceable clients'.format(client_ip_address))

            return requests.codes.BAD_REQUEST, None

        logger.debug('Proxying request for {0} from {1} to {2}'.format(path, client_ip_address, full_url))

        smooth_streams_session = SmoothStreamsProxy.get_session_parameter('http_session')
        parsed_url = urllib.parse.urlparse(full_url)

        response = SmoothStreamsProxy.make_http_request(
            smooth_streams_session.get,
            '{0}://{1}{2}'.format(parsed_url.scheme,
                                  parsed_url.netloc,
                                  parsed_url.path),
            params=dict(urllib.parse.parse_qsl(parsed_url.query)),
            headers=smooth_streams_session.headers,
            cookies=smooth_streams_session.cookies.get_dict())

        response_status_code = response.status_code
        if response_status_code == requests.codes.ok:
            response_headers = response.headers
            response_text = response.text

            logger.trace(
                'Response from {0}:\n'
                '[Status Code]\n=============\n{1}\n\n'
                '[Header]\n========\n{2}\n\n'
                '[Content]\n=========\n{3}\n'.format(full_url, response_status_code,
                                                     '\n'.join(['{0:32} => {1!s}'.format(
                                                         key,
                                                         response_headers[key])
                                                         for key in sorted(response_headers)]),
                                                     response_text))

            return response_status_code, response_text
        else:
            logger.error(
                'HTTP error {0} encountered requesting {1} for {2}'.format(response_status_code, full_url,
                                                                           client_ip_address))

            return response_status_code, None

    @classmethod
    def _download_ts_file(cls, path, client_ip_address):
        try:
            full_url = '{0}{1}'.format(
                SmoothStreamsProxy.get_serviceable_client_parameter(client_ip_address,
                                                                    'last_requested_channel_url'),
                path)
        except KeyError:
            logger.error('Client {0} not in serviceable clients'.format(client_ip_address))

            return requests.codes.BAD_REQUEST, None

        logger.debug('Proxying request for {0} from {1} to {2}'.format(path, client_ip_address, full_url))

        smooth_streams_session = SmoothStreamsProxy.get_session_parameter('http_session')
        parsed_url = urllib.parse.urlparse(full_url)

        response = SmoothStreamsProxy.make_http_request(smooth_streams_session.get,
                                                        '{0}://{1}{2}'.format(parsed_url.scheme,
                                                                              parsed_url.netloc,
                                                                              parsed_url.path),
                                                        params=dict(urllib.parse.parse_qsl(parsed_url.query)),
                                                        headers=smooth_streams_session.headers,
                                                        cookies=smooth_streams_session.cookies.get_dict())

        response_status_code = response.status_code
        if response_status_code == requests.codes.ok:
            response_headers = response.headers
            response_content = response.content

            logger.trace(
                'Response from {0}:\n'
                '[Status Code]\n=============\n{1}\n\n'
                '[Header]\n========\n{2}\n\n'
                '[Content]\n=========\n{3:,}\n'.format(full_url,
                                                       response_status_code,
                                                       '\n'.join(['{0:32} => {1!s}'.format(
                                                           key,
                                                           response_headers[key])
                                                           for key in sorted(response_headers)]),
                                                       len(response_content)))

            return response_status_code, response_content
        else:
            logger.error(
                'HTTP error {0} encountered requesting {1} for {2}'.format(response_status_code, full_url,
                                                                           client_ip_address))

            return response_status_code, None

    def _generate_playlist_m3u8(self, path, client_ip_address, protocol):
        SmoothStreamsProxy.refresh_serviceable_clients(client_ip_address)

        channels_file_name = 'channels.json'

        with SmoothStreamsProxyHTTPRequestHandler._lock:
            if SmoothStreamsProxy.do_download_file(channels_file_name):
                url = '{0}/{1}'.format(SmoothStreamsProxy.get_epg_source_url(), channels_file_name)

                http_response_status_code = SmoothStreamsProxy.download_file(channels_file_name,
                                                                             url,
                                                                             do_gunzip=False)
                if http_response_status_code != requests.codes.ok:
                    logger.error(
                        'HTTP error {0} encountered requesting {1} for {2}'.format(http_response_status_code,
                                                                                   url,
                                                                                   client_ip_address))

                    self.send_error(http_response_status_code)

                    return

        playlist_m3u8 = SmoothStreamsProxy.generate_playlist_m3u8(protocol)
        self._send_http_response(client_ip_address,
                                 path,
                                 requests.codes.OK,
                                 SmoothStreamsProxyHTTPRequestHandler._prepare_response_headers(
                                     playlist_m3u8,
                                     'application/vnd.apple.mpegurl'),
                                 playlist_m3u8)

    @classmethod
    def _parse_query_string(cls, path, parameters_default_values_map):
        query_string = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)

        return [
            ''.join(query_string[parameter]) if parameter in query_string else parameters_default_values_map[parameter]
            for parameter in sorted(parameters_default_values_map)]

    @classmethod
    def _prepare_response_headers(cls, response_content, content_type):
        return {'Accept-Ranges': 'bytes',
                'Content-Length': '{0}'.format(len(response_content)),
                'Content-Type': content_type,
                'Date': formatdate(usegmt=True),
                'Server': 'SmoothStreamsProxy'
                }

    def _send_http_response(self, client_ip_address, path, response_status_code, response_headers, response_content,
                            do_print_content=True):
        self.send_response(requests.codes.OK)

        headers = []
        for header_entry in sorted(response_headers):
            self.send_header(header_entry, response_headers[header_entry])
            headers.append(
                '{0:32} => {1!s}'.format(header_entry, response_headers[header_entry]))
        self.end_headers()

        logger.trace(
            'Response to {0} for {1}:\n'
            '[Status Code]\n=============\n{2}\n\n'
            '[Header]\n========\n{3}\n\n'
            '[Content]\n=========\n{4:{5}}\n'.format(client_ip_address,
                                                     path,
                                                     response_status_code,
                                                     '\n'.join(headers),
                                                     response_content if do_print_content else len(response_content),
                                                     '' if do_print_content else ','))
        try:
            self.wfile.write(bytes(response_content, 'utf-8'))
        except TypeError:
            self.wfile.write(response_content)

    def do_GET(self):
        client_address = self.client_address
        client_ip_address = client_address[0]
        path = self.path

        logger.debug('{0} requested from {1}'.format(path, client_ip_address))

        if path.find('playlist.m3u8') != -1:
            protocol = SmoothStreamsProxy.get_configuration_parameter('SMOOTH_STREAMS_PROTOCOL')
            do_generate_playlist_m3u8 = False

            if '?' in path:
                channel_number, protocol = SmoothStreamsProxyHTTPRequestHandler._parse_query_string(
                    path,
                    {'channel_number': None,
                     'protocol': protocol})

                if channel_number:
                    logger.info(
                        '{0} requested from {1}'.format(SmoothStreamsProxy.get_channel_name(int(channel_number)),
                                                        client_ip_address))

                    SmoothStreamsProxy.refresh_serviceable_clients(client_ip_address)
                    SmoothStreamsProxy.set_serviceable_client_parameter(
                        client_ip_address,
                        'last_requested_channel_url',
                        'https://{0}.smoothstreams.tv/{1}/ch{2}q1.stream'.format(
                            SmoothStreamsProxy.get_configuration_parameter('SMOOTH_STREAMS_SERVER'),
                            SmoothStreamsProxy.get_configuration_parameter('SMOOTH_STREAMS_SERVICE'),
                            channel_number)
                    )

                    with SmoothStreamsProxyHTTPRequestHandler._lock:
                        try:
                            SmoothStreamsProxy.refresh_session()
                        except SmoothStreamsProxyException:
                            self.send_error(SmoothStreamsProxy.get_session_parameter('http_response_status_code'))

                            return

                        smooth_streams_hash = SmoothStreamsProxy.get_session_parameter('hash')

                    if protocol == 'hls':
                        response_status_code, response_text = \
                            SmoothStreamsProxyHTTPRequestHandler._download_playlist_m3u8(path,
                                                                                         client_ip_address,
                                                                                         smooth_streams_hash)

                        if response_status_code == requests.codes.ok:
                            self._send_http_response(client_ip_address,
                                                     path,
                                                     response_status_code,
                                                     SmoothStreamsProxyHTTPRequestHandler._prepare_response_headers(
                                                         response_text,
                                                         'application/vnd.apple.mpegurl'),
                                                     response_text)
                        else:
                            self.send_error(response_status_code)
                    elif protocol == 'rtmp':
                        response_text = '#EXTM3U\n' \
                                        '#EXTINF:-1 ,{0}\n' \
                                        'rtmp://{1}.smoothstreams.tv:3635/{2}?wmsAuthSign={3}/ch{4}q1.stream'.format(
                            SmoothStreamsProxy.get_channel_name(int(channel_number)),
                            SmoothStreamsProxy.get_configuration_parameter('SMOOTH_STREAMS_SERVER'),
                            SmoothStreamsProxy.get_configuration_parameter('SMOOTH_STREAMS_SERVICE'),
                            smooth_streams_hash,
                            channel_number)

                        self._send_http_response(client_ip_address,
                                                 path,
                                                 requests.codes.OK,
                                                 SmoothStreamsProxyHTTPRequestHandler._prepare_response_headers(
                                                     response_text,
                                                     'application/vnd.apple.mpegurl'),
                                                 response_text)
                    else:
                        logger.error('protocol: {0} sent in the query string is not supported'.format(protocol))

                        self.send_error(requests.codes.BAD_REQUEST)
                elif protocol:
                    do_generate_playlist_m3u8 = True
                else:
                    logger.error('Request with invalid query string')

                    self.send_error(requests.codes.BAD_REQUEST)

                    return
            else:
                do_generate_playlist_m3u8 = True

            if do_generate_playlist_m3u8:
                self._generate_playlist_m3u8(path, client_ip_address, protocol)
        elif path.find('epg.xml') != -1:
            number_of_days, = SmoothStreamsProxyHTTPRequestHandler._parse_query_string(path, {'number_of_days': 1})
            channels_file_name = 'xmltv{0}.xml'.format(number_of_days)

            with SmoothStreamsProxyHTTPRequestHandler._lock:
                if SmoothStreamsProxy.do_download_file(channels_file_name):
                    url = '{0}/{1}.gz'.format(SmoothStreamsProxy.get_epg_source_url(), channels_file_name)

                    http_response_status_code = SmoothStreamsProxy.download_file(channels_file_name,
                                                                                 url,
                                                                                 do_gunzip=True)
                    if http_response_status_code != requests.codes.ok:
                        logger.error(
                            'HTTP error {0} encountered requesting {1} for {2}'.format(http_response_status_code,
                                                                                       url,
                                                                                       client_ip_address))

                        self.send_error(http_response_status_code)

                        return

            epg = SmoothStreamsProxy.get_file_contents(channels_file_name)
            self._send_http_response(client_ip_address,
                                     path,
                                     requests.codes.OK,
                                     SmoothStreamsProxyHTTPRequestHandler._prepare_response_headers(
                                         epg,
                                         'application/xml'),
                                     epg,
                                     do_print_content=False)
        elif path.find('chunks.m3u8') != -1:
            nimble_session_id_in_request, smooth_streams_hash_in_request = \
                SmoothStreamsProxyHTTPRequestHandler._parse_query_string(path,
                                                                         {'nimblesessionid': None,
                                                                          'wmsAuthSign': None})

            smooth_streams_hash = SmoothStreamsProxy.get_session_parameter('hash')

            if smooth_streams_hash_in_request != smooth_streams_hash:
                nimble_session_id = SmoothStreamsProxy.get_nimble_session_id(nimble_session_id_in_request)

                if not nimble_session_id:
                    logger.debug('Authorization hash {0} in request from {1} expired'.format(
                        smooth_streams_hash_in_request,
                        client_ip_address))

                    response_status_code, response_text = SmoothStreamsProxyHTTPRequestHandler._download_playlist_m3u8(
                        path,
                        client_ip_address,
                        smooth_streams_hash)

                    if response_status_code == requests.codes.ok:
                        m3u8_obj = m3u8.loads(response_text)

                        path = '/{0}'.format(m3u8_obj.data['playlists'][0]['uri'])
                        nimble_session_id, = SmoothStreamsProxyHTTPRequestHandler._parse_query_string(
                            path,
                            {'nimblesessionid': None})

                        SmoothStreamsProxy.hijack_session_id(nimble_session_id_in_request, nimble_session_id)
                    else:
                        self.send_error(response_status_code)

                        return
                else:
                    path = '/chunks.m3u8?nimblesessionid={0}&&wmsAuthSign={1}'.format(nimble_session_id,
                                                                                      smooth_streams_hash)

            response_status_code, response_text = SmoothStreamsProxyHTTPRequestHandler._download_chunks_m3u8(
                path,
                client_ip_address)

            if response_status_code == requests.codes.ok:
                self._send_http_response(client_ip_address,
                                         path,
                                         response_status_code,
                                         SmoothStreamsProxyHTTPRequestHandler._prepare_response_headers(
                                             response_text,
                                             'application/vnd.apple.mpegurl'),
                                         response_text)
            else:
                self.send_error(response_status_code)
        elif path.find('.ts') != -1:
            response_status_code, response_content = SmoothStreamsProxyHTTPRequestHandler._download_ts_file(
                path,
                client_ip_address)

            if response_status_code == requests.codes.ok:
                self._send_http_response(client_ip_address,
                                         path,
                                         response_status_code,
                                         SmoothStreamsProxyHTTPRequestHandler._prepare_response_headers(
                                             response_content,
                                             'video/m2ts'),
                                         response_content,
                                         False)
            else:
                self.send_error(response_status_code)
        else:
            logger.error(
                'HTTP error {0} encountered requesting {1} for {2}'.format(requests.codes.NOT_FOUND,
                                                                           path,
                                                                           client_ip_address))

            self.send_error(requests.codes.NOT_FOUND)

            return

    def log_message(self, format_, *args):
        return


class SmoothStreamsProxyHTTPRequestHandlerThread(threading.Thread):
    def __init__(self, server_address, server_socket):
        threading.Thread.__init__(self)

        self.server_address = server_address
        self.server_socket = server_socket

        self.daemon = True
        self.start()

    def run(self):
        smooth_streams_proxy_http_server = HTTPServer(self.server_address, SmoothStreamsProxyHTTPRequestHandler, False)

        smooth_streams_proxy_http_server.socket = self.server_socket
        smooth_streams_proxy_http_server.server_bind = self.server_close = lambda self: None

        smooth_streams_proxy_http_server.serve_forever()


class SmoothStreamsProxyConfigurationEventHandler(FileSystemEventHandler):
    _last_modification_date_time = None
    _lock = threading.Lock()

    def on_modified(self, event):
        with SmoothStreamsProxyConfigurationEventHandler._lock:
            configuration_file = SmoothStreamsProxy.get_configuration_file()
            if event.src_path == configuration_file:
                read_configuration_file = False
                last_modification_date_time = SmoothStreamsProxyConfigurationEventHandler._last_modification_date_time
                now = datetime.now(pytz.utc)

                # Read the configuration file if this is the first modification since the proxy started or if the
                # modification events are at least 1s apart (A hack to deal with watchdog generating duplicate events)
                if not last_modification_date_time:
                    read_configuration_file = True

                    SmoothStreamsProxyConfigurationEventHandler._last_modification_date_time = now
                else:
                    total_time_between_modifications = (now - last_modification_date_time).total_seconds()

                    if total_time_between_modifications >= 1.0:
                        read_configuration_file = True

                        SmoothStreamsProxyConfigurationEventHandler._last_modification_date_time = now

                if read_configuration_file:
                    SmoothStreamsProxy.read_configuration_file(initial_read=False)

                    try:
                        SmoothStreamsProxy.set_logging_level(getattr(logging,
                                                                     SmoothStreamsProxy.get_configuration_parameter(
                                                                         'LOGGING_LEVEL').upper()))
                    except AttributeError:
                        pass


class SmoothStreamsProxy:
    _channel_map = {}
    _channel_map_lock = threading.Lock()
    _configuration = {}
    _configuration_lock = threading.Lock()
    _epg_source_urls = ['https://sstv.fog.pt/epg', 'http://ca.epgrepo.download', 'http://eu.epgrepo.download']
    _fernet_key = None
    _files_map = {}
    _files_map_lock = threading.Lock()
    _http_request_handler_threads = None
    _log_file = None
    _nimble_session_id_map = {}
    _nimble_session_id_map_lock = threading.Lock()
    _refresh_session_timer = None
    _serviceable_clients = {}
    _serviceable_clients_lock = threading.Lock()
    _session = {}
    _session_lock = threading.Lock()
    _configuration_file = None

    @classmethod
    def _cleanup_shelf(cls):
        file_path = os.path.join(os.getcwd(), 'smooth_streams_proxy_db')

        logger.debug('Cleaning up shelved settings from {0}'.format(file_path))

        with cls._session_lock:
            try:
                with shelve.open(file_path) as smooth_streams_proxy_db:
                    session = smooth_streams_proxy_db['session']
                    hash_expires_on = session['expires_on']
                    now_utc = datetime.now(pytz.utc)

                    if now_utc >= hash_expires_on:
                        logger.debug('Deleting expired shelved session:\nHash => {0}\nExpired On => {1}'.format(
                            session['hash'],
                            session['expires_on'].astimezone(get_localzone()).strftime('%Y-%m-%d %H:%M:%S')[:-3]))
                        del smooth_streams_proxy_db['session']

                    files_map = smooth_streams_proxy_db['files_map']
                    for file_name in sorted(files_map):
                        next_update_date_time = files_map[file_name]['next_update_date_time']
                        now_utc = datetime.now(pytz.utc)

                        if now_utc >= next_update_date_time:
                            logger.debug('Deleting expired shelved file:\n{0}'.format(
                                '\n'.join(
                                    ['File name  => {0}\nFile size  => {1:,}\nExpired on => {2}\n'.format(
                                        file_name,
                                        len(files_map[file_name]['content']),
                                        next_update_date_time.astimezone(get_localzone()).strftime(
                                            '%Y-%m-%d %H:%M:%S'))
                                        for file_name in sorted(files_map)]).strip()))

                            del files_map[file_name]
                    smooth_streams_proxy_db['files_map'] = files_map
            except KeyError:
                pass

    @classmethod
    def _decrypt_password(cls):
        with cls._configuration_lock:
            return Fernet(cls._fernet_key).decrypt(cls._configuration['SMOOTH_STREAMS_PASSWORD'].encode())

    @classmethod
    def _determine_configuration_password_state(cls):
        configuration_password_state = PasswordState.ENCRYPTED

        try:
            base64_decoded_encrypted_fernet_token = base64.urlsafe_b64decode(
                cls._configuration['SMOOTH_STREAMS_PASSWORD'])

            if base64_decoded_encrypted_fernet_token[0] == 0x80:
                length_of_base64_decoded_encrypted_fernet_token = len(base64_decoded_encrypted_fernet_token)

                if length_of_base64_decoded_encrypted_fernet_token < 73 or (
                        length_of_base64_decoded_encrypted_fernet_token - 57) % 16 != 0:
                    configuration_password_state = PasswordState.DECRYPTED
            else:
                configuration_password_state = PasswordState.DECRYPTED
        except binascii.Error:
            configuration_password_state = PasswordState.DECRYPTED

        return configuration_password_state

    @classmethod
    def _do_retrieve_authorization_hash(cls):
        try:
            hash_expires_on = cls._session['expires_on']
            now_utc = datetime.now(pytz.utc)

            if now_utc < (hash_expires_on - timedelta(seconds=60)):
                return False
            else:
                logger.info('Authorization hash for expired. Need to retrieve a new authorization hash')
                return True
        except KeyError:
            logger.debug('Authorization hash was never retrieved. Need to retrieve one')
            return True

    @classmethod
    def _encrypt_password(cls):
        cls._fernet_key = Fernet.generate_key()
        fernet = Fernet(cls._fernet_key)
        encrypted_smooth_streams_password = fernet.encrypt(
            cls._configuration['SMOOTH_STREAMS_PASSWORD'].encode()).decode()

        logger.debug('Encrypted SmoothStreams password to {0}'.format(encrypted_smooth_streams_password))

        cls._configuration['SMOOTH_STREAMS_PASSWORD'] = encrypted_smooth_streams_password

    @classmethod
    def _initialize_logging(cls):
        logging.addLevelName(TRACE, 'TRACE')
        logging.TRACE = TRACE
        logging.trace = trace
        logging.Logger.trace = trace

        formatter = MultiLineFormatter('%(asctime)s %(name)-20s %(levelname)-8s %(message)s')

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        rotating_file_handler = logging.handlers.RotatingFileHandler('{0}'.format(cls._log_file),
                                                                     maxBytes=1024 * 1024 * 10,
                                                                     backupCount=10)
        rotating_file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(rotating_file_handler)

        cls.set_logging_level(logging.INFO)

    @classmethod
    def _load_shelved_settings(cls):
        file_path = os.path.join(os.getcwd(), 'smooth_streams_proxy_db')

        logger.debug('Attempting to load shelved settings from {0}'.format(file_path))

        with shelve.open(file_path) as smooth_streams_proxy_db:
            try:
                with cls._session_lock:
                    cls._session = smooth_streams_proxy_db['session']

                    logger.debug('Loaded shelved session:\nHash     => {0}\nValid to => {1}'.format(
                        cls._session['hash'],
                        cls._session['expires_on'].astimezone(get_localzone()).strftime('%Y-%m-%d %H:%M:%S')))
            except KeyError:
                logger.debug('Failed to load shelved session from {0}'.format(
                    os.path.join(os.getcwd(), 'smooth_streams_proxy_db')))

            try:
                with cls._files_map_lock:
                    cls._files_map = smooth_streams_proxy_db['files_map']

                    if cls._files_map:
                        logger.debug('Loaded shelved files:\n{0}'.format(
                            '\n'.join(
                                ['File name => {0}\nFile size => {1:,}\nValid to  => {2}\n'.format(
                                    file_name,
                                    len(cls._files_map[file_name]['content']),
                                    cls._files_map[file_name]['next_update_date_time'].astimezone(
                                        get_localzone()).strftime('%Y-%m-%d %H:%M:%S')) for file_name
                                    in sorted(cls._files_map)]).strip()))
            except KeyError:
                logger.debug(
                    'Failed to load files from {0}'.format(os.path.join(os.getcwd(), 'smooth_streams_proxy_db')))

            try:
                with cls._channel_map_lock:
                    cls._channel_map = smooth_streams_proxy_db['channel_map']

                    logger.debug('Loaded shelved channel map:\n{0}'.format('\n'.join(['{0:03} => {1}'.format(
                        channel_number,
                        cls._channel_map[channel_number]) for channel_number in sorted(cls._channel_map)])))
            except KeyError:
                logger.debug(
                    'Failed to load channel map from {0}'.format(os.path.join(os.getcwd(), 'smooth_streams_proxy_db')))

            try:
                cls._fernet_key = smooth_streams_proxy_db['fernet_key']

                logger.debug('Loaded shelved decryption key')
            except KeyError:
                logger.debug('Failed to load decryption key from {0}'.format(os.path.join(os.getcwd(),
                                                                                          'smooth_streams_proxy_db')))

    @classmethod
    def _manage_password(cls):
        if cls._determine_configuration_password_state() == PasswordState.DECRYPTED:
            cls._scrub_configuration_file()
        else:
            if cls._fernet_key:
                cls._validate_fernet_key()
            else:
                logger.debug(
                    'SmoothStreams password is encrypted, but no decryption key was found\n'
                    'Please re-enter your cleartext password in the configuration file {0}\n'
                    'Exiting...'.format(cls._configuration_file))

                sys.exit()

    @classmethod
    def _persist_to_shelf(cls, key, value):
        file_path = os.path.join(os.getcwd(), 'smooth_streams_proxy_db')

        logger.debug('Attempting to persist {0} to {1}'.format(key, file_path))

        try:
            with shelve.open(file_path) as smooth_streams_proxy_db:
                smooth_streams_proxy_db[key] = value

                logger.debug('Persisted {0} to {1}'.format(key, file_path))
        except IOError:
            logger.debug('Failed to persist {0} to {1}'.format(key, file_path))

    @classmethod
    def _process_authorization_hash(cls, hash_response):
        if 'code' in hash_response and hash_response['code'] == '1':
            cls._session['hash'] = hash_response['hash']
            cls._session['expires_on'] = datetime.now(pytz.utc) + timedelta(
                seconds=(hash_response['valid'] * 60))

            logger.info('Retrieved authorization token:\nHash => {0}\nExpires On => {1}'.format(
                cls._session['hash'],
                cls._session['expires_on'].astimezone(get_localzone()).strftime('%Y-%m-%d %H:%M:%S')[:-3]))

            cls._persist_to_shelf('session', cls._session)
        else:
            raise SmoothStreamsProxyException

    @classmethod
    def _remove_from_shelf(cls, key):
        file_path = os.path.join(os.getcwd(), 'smooth_streams_proxy_db')

        logger.debug('Attempting to remove {0} from {1}'.format(key, file_path))

        try:
            with shelve.open(file_path) as smooth_streams_proxy_db:
                del smooth_streams_proxy_db[key]
        except IOError:
            logger.debug('Failed to remove {0} from {1}'.format(key, file_path))

    @classmethod
    def _retrieve_authorization_hash(cls):
        logger.debug('Retrieving authorization hash')

        cls._session['http_session'] = None
        session = requests.Session()

        if cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE') == 'viewmmasr':
            url = 'https://www.mma-tv.net/loginForm.php'
        else:
            url = 'https://auth.smoothstreams.tv/hash_api.php'

        response = cls.make_http_request(
            session.get,
            url,
            params={
                'username': cls.get_configuration_parameter('SMOOTH_STREAMS_USERNAME'),
                'password': cls._decrypt_password().decode(),
                'site': cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE')},
            headers=session.headers,
            cookies=session.cookies.get_dict())

        response_status_code = response.status_code
        cls._session['http_response_status_code'] = response_status_code

        if response.status_code == requests.codes.ok:
            cls._session['http_session'] = session

            response_headers = response.headers
            logger.trace(
                'Response from {0}:\n'
                '[Status Code]\n=============\n{1}\n\n'
                '[Header]\n========\n{2}\n\n'
                '[Content]\n=========\n{3}\n'.format(url, response_status_code,
                                                     '\n'.join(
                                                         ['{0:32} => {1!s}'.format(key, response_headers[key])
                                                          for key in sorted(response_headers)]),
                                                     json.dumps(response.json(), sort_keys=True,
                                                                indent=2)))

            cls._process_authorization_hash(response.json())
        else:
            logger.error(
                'HTTP error {0} encountered requesting {1}'.format(response_status_code,
                                                                   url))

            raise SmoothStreamsProxyException

    @classmethod
    def _scrub_configuration_file(cls):
        cls._encrypt_password()
        cls._update_configuration_file('SmoothStreams',
                                       'password',
                                       cls._configuration['SMOOTH_STREAMS_PASSWORD'])
        cls._persist_to_shelf('fernet_key', cls._fernet_key)

    @classmethod
    def _set_channel_map(cls, channel_map):
        with cls._channel_map_lock:
            cls._channel_map = channel_map

            cls._persist_to_shelf('channel_map', cls._channel_map)

    @classmethod
    def _timed_refresh_session(cls):
        logger.debug('Authorization hash refresh timer triggered')

        cls.refresh_session()

    @classmethod
    def _update_configuration_file(cls, section, option, value):
        with cls._configuration_lock:
            configuration = configparser.RawConfigParser()
            configuration.read(cls._configuration_file)

            configuration.set(section, option, value)

            with open(cls._configuration_file, 'w') as configuration_file:
                configuration.write(configuration_file)

                logger.debug('Updated configuration file {0}\nSection => {1}\nOption  => {2}\nValue  => {3}'.format(
                    cls._configuration_file, section, option, value))

    @classmethod
    def _validate_fernet_key(cls):
        try:
            cls._decrypt_password()

            logger.debug('Decryption key loaded is valid for the encrypted SmoothStreams password')
        except InvalidToken:
            logger.debug(
                'Decryption key loaded is not valid for the encrypted SmoothStreams password\n'
                'Please re-enter your cleartext password in the configuration file {0}\n'
                'Exiting...'.format(cls._configuration_file))

            cls._remove_from_shelf('fernet_key')

            sys.exit()

    @classmethod
    def do_download_file(cls, file_name):
        do_download = False

        with cls._files_map_lock:
            if file_name in cls._files_map:
                if datetime.now(pytz.utc) > cls._files_map[file_name]['next_update_date_time']:
                    logger.info('{0} is stale. Will retrieve it.'.format(file_name))
                    do_download = True
            else:
                logger.info('{0} was never retrieved. Will retrieve it.'.format(file_name))
                do_download = True

        return do_download

    @classmethod
    def download_file(cls, file_name, url, do_gunzip):
        logger.info('Downloading {0}'.format(url))

        session = requests.Session()
        response = cls.make_http_request(session.get, url, headers=session.headers)

        response_status_code = response.status_code
        if response_status_code == requests.codes.ok:
            response_headers = response.headers
            response_content = response.content if do_gunzip else response.json()

            logger.trace(
                'Response from {0}:\n'
                '[Status Code]\n=============\n{1}\n\n'
                '[Header]\n========\n{2}\n\n'
                '[Content]\n=========\n{3:,}\n'.format(url, response_status_code, '\n'.join(
                    ['{0:32} => {1!s}'.format(key, response_headers[key])
                     for key in sorted(response_headers)]), len(response_content)))

            if do_gunzip:
                try:
                    response_content = '<?xml version="1.0" encoding="UTF-8"?>{0}'.format(
                        gzip.decompress(response_content).decode('utf-8'))
                except OSError:
                    response_content = '<?xml version="1.0" encoding="UTF-8"?>{0}'.format(
                        response_content.decode('utf-8'))

            if logger.getEffectiveLevel() <= logging.DEBUG:
                file_path = os.path.join(os.getcwd(), 'logs', file_name)
                with open(file_path, 'w') as out_file:
                    if do_gunzip:
                        document = etree.fromstring(response_content[38:])
                        out_file.write(etree.tostring(document,
                                                      encoding='utf-8',
                                                      xml_declaration=True,
                                                      pretty_print=True).decode('utf-8'))

                        logger.debug('EPG written to {0}'.format(file_path))
                    else:
                        out_file.write(json.dumps(response_content, sort_keys=True, indent=2))

                        logger.debug(
                            'Channels written to {0}'.format(os.path.join(os.getcwd(), 'logs', 'channels.json')))

            now = datetime.now(pytz.utc)

            cls._files_map[file_name] = {}
            cls._files_map[file_name]['content'] = response_content
            cls._files_map[file_name]['next_update_date_time'] = now.replace(
                microsecond=0,
                second=0,
                minute=0,
                hour=0) + timedelta(
                hours=(((now.hour // 4) * 4) + 3) + ((now.minute // 33) * (4 if (now.hour + 1) % 4 == 0 else 0)),
                minutes=32)

            cls._persist_to_shelf('files_map', cls._files_map)

        return response_status_code

    @classmethod
    def generate_playlist_m3u8(cls, protocol):
        with cls._files_map_lock:
            channels_json = cls._files_map['channels.json']['content']

        try:
            channel_map = {}
            playlist_m3u8 = []

            for channel_key in channels_json:
                channel = channels_json[channel_key]

                group_title = '24/7 Channels' if channel['247'] == 1 else 'Empty Channels'
                tvg_id = channel['xmltvid']
                channel_number = channel['channum']
                channel_name = channel['channame']

                channel_map[int(channel_number)] = channel_name

                playlist_m3u8.append(
                    '#EXTINF:-1 group-title="{0}" '
                    'tvg-id="{1}" '
                    'tvg-name="{3}" '
                    'tvg-logo="http://speed.guide.smoothstreams.tv/assets/images/channels/{2}.png" '
                    'channel-id="{2}",{3}\n'
                    'http://{4}:{5}/playlist.m3u8?channel_number={6:02}&protocol={7}\n'.format(
                        group_title,
                        tvg_id,
                        channel_number,
                        channel_name,
                        cls._configuration['SERVER_HOST'],
                        cls._configuration['SERVER_PORT'],
                        int(channel_number),
                        protocol))

            cls._set_channel_map(channel_map)

            playlist_m3u8 = '#EXTM3U x-tvg-url="http://{0}:{1}/epg.xml"\n{2}'.format(
                cls._configuration['SERVER_HOST'],
                cls._configuration['SERVER_PORT'],
                ''.join(playlist_m3u8))

            logger.debug('Generated playlist.m3u8')

            return playlist_m3u8

        except requests.exceptions.RequestException:
            pass
        except ValueError:
            pass

    @classmethod
    def get_channel_name(cls, channel_number):
        with cls._channel_map_lock:
            return cls._channel_map[channel_number] if channel_number in cls._channel_map else 'Channel {0:02}'.format(
                channel_number)

    @classmethod
    def get_configuration_file(cls):
        return cls._configuration_file

    @classmethod
    def get_configuration_parameter(cls, parameter_name):
        with cls._configuration_lock:
            return cls._configuration[parameter_name]

    @classmethod
    def get_epg_source_url(cls):
        return cls._epg_source_urls[random.randint(0, len(cls._epg_source_urls) - 1)]

    @classmethod
    def get_file_contents(cls, epg_file_name):
        with cls._files_map_lock:
            return cls._files_map[epg_file_name]['content']

    @classmethod
    def get_nimble_session_id(cls, hijacked_nimble_session_id):
        with cls._nimble_session_id_map_lock:
            return cls._nimble_session_id_map.get(hijacked_nimble_session_id, None)

    @classmethod
    def get_serviceable_client_parameter(cls, client_ip_address, parameter_name):
        with cls._serviceable_clients_lock:
            return cls._serviceable_clients[client_ip_address][parameter_name]

    @classmethod
    def get_session_parameter(cls, parameter_name):
        with cls._session_lock:
            return cls._session[parameter_name]

    @classmethod
    def hijack_session_id(cls, hijacked_nimble_session_id, hijacking_nimble_session_id):
        with cls._nimble_session_id_map_lock:
            cls._nimble_session_id_map[hijacked_nimble_session_id] = hijacking_nimble_session_id

    @classmethod
    def make_http_request(cls, requests_http_method, url, params=None, data=None, json_=None, headers=None,
                          cookies=None, timeout=60):
        try:
            logger.trace('Request:\n[Method]\n========\n{0}\n\n[URL]\n=====\n{1}\n{2}{3}{4}{5}'.format(
                requests_http_method.__name__.capitalize(),
                url,
                '\n[Parameters]\n============\n{0}\n'.format('\n'.join(
                    ['{0:32} => {1!s}'.format(key, params[key]) for key in sorted(params)])) if params else '',
                '\n[Headers]\n=========\n{0}\n'.format('\n'.join(
                    ['{0:32} => {1!s}'.format(key, pprint.pformat(headers[key], indent=2)) for key in
                     sorted(headers)])) if headers else '',
                '\n[Cookies]\n=========\n{0}\n'.format('\n'.join(
                    ['{0:32} => {1!s}'.format(key, pprint.pformat(cookies[key], indent=2)) for key in
                     sorted(cookies)])) if cookies else '',
                '\n[JSON]\n======\n{0}\n'.format(json.dumps(json_, sort_keys=True, indent=2)) if json_ else '').strip())

            return requests_http_method(url,
                                        params=params,
                                        data=data,
                                        json=json_,
                                        headers=headers,
                                        cookies=cookies,
                                        timeout=timeout)
        except requests.exceptions.RequestException:
            pass

    @classmethod
    def read_configuration_file(cls, initial_read=True):
        with cls._configuration_lock:
            configuration = configparser.RawConfigParser()
            configuration.read(cls._configuration_file)

            error_in_configuration_file = False
            error_messages = []

            server_hostname = None
            server_port = None
            smooth_streams_service = None
            smooth_streams_server = None
            smooth_streams_username = None
            smooth_streams_password = None
            smooth_streams_protocol = None
            logging_level = None

            try:
                server_section = configuration['Server']

                try:
                    server_hostname = server_section['hostname']
                except KeyError:
                    error_in_configuration_file = True

                    error_messages.append('Could not find a hostname option within the [Server] section\n')

                try:
                    server_port = server_section['port']

                    port = int(server_port)
                    if port < 0 or port > 65535:
                        error_in_configuration_file = True

                        error_messages.append(
                            'The port option within the [Server] section must be a number between 0 and 65535\n')
                except KeyError:
                    error_in_configuration_file = True

                    error_messages.append(
                        'Could not find a port option within the [Server] section\n'
                        'The port option within the [Server] section must be a number between 0 and 65535\n')
                except ValueError:
                    error_in_configuration_file = True

                    error_messages.append(
                        'The port option within the [Server] section must be a number between 0 and 65535\n')
            except KeyError:
                error_in_configuration_file = True

                error_messages.append('Could not find a [Server] section\n')

            try:
                smooth_streams_section = configuration['SmoothStreams']

                try:
                    smooth_streams_service = smooth_streams_section['service']

                    if smooth_streams_service.lower() not in VALID_SMOOTH_STREAMS_SERVICE_VALUES:
                        error_in_configuration_file = True

                        error_messages.append(
                            'The service option within the [SmoothStreams] section must be one of\n{0}\n'.format(
                                '\n'.join(
                                    ['\u2022 {0}'.format(service) for service in VALID_SMOOTH_STREAMS_SERVICE_VALUES])))
                except KeyError:
                    error_in_configuration_file = True

                    error_messages.append(
                        'Could not find a service option within the [SmoothStreams] section\n'
                        'The service option within the [SmoothStreams] section must be one of\n{0}\n'.format(
                            '\n'.join(
                                ['\u2022 {0}'.format(service) for service in VALID_SMOOTH_STREAMS_SERVICE_VALUES])))

                try:
                    smooth_streams_server = smooth_streams_section['server']

                    if smooth_streams_server.lower() not in VALID_SMOOTH_STREAMS_SERVER_VALUES:
                        error_in_configuration_file = True

                        error_messages.append(
                            'The server option within the [SmoothStreams] section must be one of\n{0}\n'.format(
                                '\n'.join(
                                    ['\u2022 {0}'.format(service) for service in VALID_SMOOTH_STREAMS_SERVER_VALUES])))
                except KeyError:
                    error_in_configuration_file = True

                    error_messages.append(
                        'Could not find a server option within the [SmoothStreams] section\n'
                        'The server option within the [SmoothStreams] section must be one of\n{0}\n'.format(
                            '\n'.join(
                                ['\u2022 {0}'.format(service) for service in VALID_SMOOTH_STREAMS_SERVER_VALUES])))

                try:
                    smooth_streams_username = smooth_streams_section['username']
                except KeyError:
                    error_in_configuration_file = True

                    error_messages.append('Could not find a username option within the [SmoothStreams] section\n')

                try:
                    smooth_streams_password = smooth_streams_section['password']
                except KeyError:
                    error_in_configuration_file = True

                    error_messages.append('Could not find a password option within the [SmoothStreams] section\n')

                try:
                    smooth_streams_protocol = smooth_streams_section['protocol']

                    if smooth_streams_protocol.lower() not in VALID_SMOOTH_STREAMS_PROTOCOL_VALUES:
                        error_in_configuration_file = True

                        error_messages.append(
                            'The protocol option within the [SmoothStreams] section must be one of\n{0}\n'.format(
                                '\n'.join(
                                    ['\u2022 {0}'.format(service) for service in
                                     VALID_SMOOTH_STREAMS_PROTOCOL_VALUES])))
                except KeyError:
                    error_in_configuration_file = True

                    error_messages.append(
                        'Could not find a protocol option within the [SmoothStreams] section\n'
                        'The protocol option within the [SmoothStreams] section must be one of\n{0}\n'.format(
                            '\n'.join(
                                ['\u2022 {0}'.format(service) for service in VALID_SMOOTH_STREAMS_PROTOCOL_VALUES])))
            except KeyError:
                error_in_configuration_file = True

                error_messages.append('Could not find a [SmoothStreams] section\n')

            try:
                logging_section = configuration['Logging']

                logging_level = logging_section['level']

                if logging_level.upper() not in VALID_LOGGING_LEVEL_VALUES:
                    logging_level = 'INFO'
            except KeyError:
                logging_level = 'INFO'

            if error_in_configuration_file:
                error_messages.insert(0, 'Configuration file => {0}'.format(cls._configuration_file))
                error_messages.append('Exiting...') if initial_read else error_messages.append('Skipping...')

                logger.error('\n'.join(error_messages))

                if initial_read:
                    sys.exit()
            else:
                cls._configuration['SERVER_HOST'] = server_hostname
                cls._configuration['SERVER_PORT'] = server_port
                cls._configuration['SMOOTH_STREAMS_SERVICE'] = smooth_streams_service
                cls._configuration['SMOOTH_STREAMS_SERVER'] = smooth_streams_server
                cls._configuration['SMOOTH_STREAMS_USERNAME'] = smooth_streams_username
                cls._configuration['SMOOTH_STREAMS_PASSWORD'] = smooth_streams_password
                cls._configuration['SMOOTH_STREAMS_PROTOCOL'] = smooth_streams_protocol
                cls._configuration['LOGGING_LEVEL'] = logging_level

                logger.info('{0}ead configuration file {1}'.format('R' if initial_read else 'Rer',
                                                                   cls._configuration_file))
                logger.info('SERVER_HOST = {0}'.format(cls._configuration['SERVER_HOST']))
                logger.info('SERVER_PORT = {0}'.format(cls._configuration['SERVER_PORT']))
                logger.info('SMOOTH_STREAMS_SERVICE = {0}'.format(cls._configuration['SMOOTH_STREAMS_SERVICE']))
                logger.info('SMOOTH_STREAMS_SERVER = {0}'.format(cls._configuration['SMOOTH_STREAMS_SERVER']))
                logger.info('SMOOTH_STREAMS_USERNAME = {0}'.format(cls._configuration['SMOOTH_STREAMS_USERNAME']))
                logger.info('SMOOTH_STREAMS_PASSWORD = {0}'.format(cls._configuration['SMOOTH_STREAMS_PASSWORD']))
                logger.info('SMOOTH_STREAMS_PROTOCOL = {0}'.format(cls._configuration['SMOOTH_STREAMS_PROTOCOL']))
                logger.info('LOGGING_LEVEL = {0}'.format(cls._configuration['LOGGING_LEVEL']))

    @classmethod
    def refresh_serviceable_clients(cls, client_ip_address):
        with cls._serviceable_clients_lock:
            if client_ip_address not in cls._serviceable_clients:
                logger.debug('Adding {0} to serviceable clients'.format(client_ip_address))

                cls._serviceable_clients[client_ip_address] = {}
            else:
                cls._serviceable_clients[client_ip_address]['last_request_date_time'] = datetime.now(pytz.utc)

    @classmethod
    def refresh_session(cls):
        with cls._session_lock:
            do_start_timer = False

            if cls._do_retrieve_authorization_hash():
                do_start_timer = True

                cls._retrieve_authorization_hash()

                if cls._refresh_session_timer:
                    cls._refresh_session_timer.cancel()
            elif not cls._refresh_session_timer:
                do_start_timer = True

            if do_start_timer:
                interval = (cls._session['expires_on'] - datetime.now(pytz.utc)).total_seconds() - 45

                logger.debug('Starting authorization hash refresh timer with an interval of {0} seconds'.format(
                    interval))

                cls._refresh_session_timer = threading.Timer(interval,
                                                             cls._timed_refresh_session)
                cls._refresh_session_timer.start()

    @classmethod
    def set_logging_level(cls, log_level):
        logger.setLevel(log_level)

        for handler in logger.handlers:
            handler.setLevel(log_level)

    @classmethod
    def set_serviceable_client_parameter(cls, client_ip_address, parameter_name, parameter_value):
        with cls._serviceable_clients_lock:
            cls._serviceable_clients[client_ip_address][parameter_name] = parameter_value

    @classmethod
    def start(cls, number_of_threads):
        cls._configuration_file = os.path.join(os.getcwd(), 'smooth_streams_proxy.ini')
        cls._log_file = os.path.join(os.getcwd(), 'logs', 'smooth_streams_proxy.log')
        if len(sys.argv) == 2:
            cls._configuration_file = sys.argv[1]
        elif len(sys.argv) == 3:
            cls._configuration_file = sys.argv[1]
            cls._log_file = sys.argv[2]

        cls._initialize_logging()

        cls.read_configuration_file()
        try:
            cls.set_logging_level(getattr(logging,
                                          cls._configuration['LOGGING_LEVEL'].upper()))
        except AttributeError:
            pass

        cls._cleanup_shelf()
        cls._load_shelved_settings()
        cls._manage_password()
        cls.refresh_session()

        smooth_streams_proxy_configuration_event_handler = SmoothStreamsProxyConfigurationEventHandler()
        watchdog_observer = Observer()
        watchdog_observer.schedule(smooth_streams_proxy_configuration_event_handler, os.getcwd(), recursive=False)
        watchdog_observer.start()

        logger.info('Starting SmoothStreams Proxy {0}\n\n{1}\nPlaylist URL => {2}\nEPG URL      => {3}\n{1}'.format(
            VERSION,
            '=' * len('Playlist URL => http://{0}:{1}/playlist.m3u8'.format(cls._configuration['SERVER_HOST'],
                                                                            cls._configuration['SERVER_PORT'])),
            'http://{0}:{1}/playlist.m3u8'.format(cls._configuration['SERVER_HOST'],
                                                  cls._configuration['SERVER_PORT']),
            'http://{0}:{1}/epg.xml'.format(cls._configuration['SERVER_HOST'],
                                            cls._configuration['SERVER_PORT'])))

        server_address = ('', int(cls._configuration['SERVER_PORT']))
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(server_address)
        server_socket.listen(5)

        cls._http_request_handler_threads = [
            SmoothStreamsProxyHTTPRequestHandlerThread(server_address, server_socket) for _ in range(number_of_threads)]

        for smooth_streams_proxy_http_request_handler_thread in cls._http_request_handler_threads:
            smooth_streams_proxy_http_request_handler_thread.join()
        watchdog_observer.join()


def trace(self, msg, *args, **kwargs):
    if self.isEnabledFor(TRACE):
        self._log(TRACE, msg, args, **kwargs)


def main():
    SmoothStreamsProxy.start(5)
