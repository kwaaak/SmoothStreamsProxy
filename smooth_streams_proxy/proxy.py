import copy
import gzip
import ipaddress
import json
import logging
import os
import random
import re
import shelve
import shutil
import socket
import sys
import traceback
import urllib.parse
import uuid
import warnings
from datetime import datetime
from datetime import timedelta
from threading import Event
from threading import RLock
from threading import Timer

import m3u8
import pytz
import requests
from configobj import ConfigObj
from cryptography.fernet import InvalidToken
from tzlocal import get_localzone
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .constants import DEFAULT_HOSTNAME_LOOPBACK
from .constants import DEFAULT_LOGGING_LEVEL
from .constants import TRACE
from .constants import VALID_LOGGING_LEVEL_VALUES
from .constants import VALID_SMOOTH_STREAMS_PROTOCOL_VALUES
from .constants import VALID_SMOOTH_STREAMS_SERVER_VALUES
from .constants import VALID_SMOOTH_STREAMS_SERVICE_VALUES
from .constants import VERSION
from .enums import SmoothStreamsProxyPasswordState
from .enums import SmoothStreamsProxyRecordingStatus
from .exceptions import DuplicateRecordingError
from .exceptions import RecordingNotFoundError
from .utilities import SmoothStreamsProxyUtility

warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)


class SmoothStreamsProxy():
    _active_recordings_to_recording_thread = {}
    _active_recordings_to_recording_thread_lock = RLock()
    _channel_map = {}
    _channel_map_lock = RLock()
    _configuration = {}
    _configuration_file_path = None
    _configuration_file_watchdog_observer = None
    _configuration_lock = RLock()
    _epg_source_urls = ['https://sstv.fog.pt/epg', 'http://ca.epgrepo.download', 'http://eu.epgrepo.download']
    _fernet_key = None
    _files_map = {}
    _files_map_lock = RLock()
    _http_request_handler_threads = None
    _log_file_path = None
    _nimble_session_id_map = {}
    _nimble_session_id_map_lock = RLock()
    _number_of_http_request_handler_threads = 1
    _previous_configuration = {}
    _recordings = []
    _recordings_directory_path = None
    _recordings_lock = RLock()
    _refresh_session_timer = None
    _serviceable_clients = {}
    _serviceable_clients_lock = RLock()
    _server_socket = None
    _session = {}
    _session_lock = RLock()
    _shelf_file_path = None
    _shutdown_proxy_event = Event()
    _start_recording_timer = None
    _start_recording_timer_lock = RLock()

    @classmethod
    def _add_file_to_files_map(cls, file_name, file):
        with cls._files_map_lock:
            cls._files_map[file_name] = file

            cls._persist_to_shelf('files_map', cls._files_map)

    @classmethod
    def _add_client_to_serviceable_clients(cls, client_uuid, client_ip_address):
        with cls._serviceable_clients_lock:
            cls._serviceable_clients[client_uuid] = {}
            cls._serviceable_clients[client_uuid]['ip_address'] = client_ip_address

    @classmethod
    def _backup_configuration(cls):
        with cls._configuration_lock:
            cls._previous_configuration = copy.deepcopy(cls._configuration)

    @classmethod
    def _cleanup_shelf(cls):
        logger.debug('Cleaning up shelved settings\n'
                     'Shelf file path => {0}'.format(cls._shelf_file_path))

        with shelve.open(cls._shelf_file_path) as smooth_streams_proxy_db:
            # <editor-fold desc="Cleanup session">
            try:
                session = smooth_streams_proxy_db['session']
                hash_expires_on = session['expires_on']
                current_date_time_in_utc = datetime.now(pytz.utc)

                if current_date_time_in_utc >= hash_expires_on:
                    logger.debug(
                        'Deleting expired shelved session\n'
                        'Hash => {0}\n'
                        'Expired On => {1}'.format(
                            session['hash'],
                            session['expires_on'].astimezone(get_localzone()).strftime('%Y-%m-%d %H:%M:%S')[:-3]))
                    del smooth_streams_proxy_db['session']
            except KeyError:
                pass
            # </editor-fold>

            # <editor-fold desc="Cleanup files_map">
            try:
                files_map = smooth_streams_proxy_db['files_map']
                for file_name in sorted(files_map):
                    next_update_date_time = files_map[file_name]['next_update_date_time']
                    current_date_time_in_utc = datetime.now(pytz.utc)

                    if current_date_time_in_utc >= next_update_date_time:
                        logger.debug(
                            'Deleting expired shelved file\n'.format(
                                'File name  => {0}\n'
                                'File size  => {1:,}\n'
                                'Expired on => {2}'.format(
                                    file_name,
                                    files_map[file_name]['size'],
                                    next_update_date_time.astimezone(get_localzone()).strftime('%Y-%m-%d %H:%M:%S'))))

                        try:
                            os.remove(files_map[file_name]['source'])
                        except OSError:
                            (type_, value_, traceback_) = sys.exc_info()
                            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

                        del files_map[file_name]

                smooth_streams_proxy_db['files_map'] = files_map
            except KeyError:
                pass
            # </editor-fold>

            # <editor-fold desc="Cleanup recordings">
            try:
                indices_of_recordings_to_delete = []
                recordings = smooth_streams_proxy_db['recordings']
                for (recording_index, recording) in enumerate(recordings):
                    current_date_time_in_utc = datetime.now(pytz.utc)
                    recording_end_date_time_in_utc = recording.end_date_time_in_utc

                    if current_date_time_in_utc >= recording_end_date_time_in_utc:
                        logger.debug(
                            'Deleting expired recording\n'
                            'Channel name      => {0}\n'
                            'Channel number    => {1}\n'
                            'Program title     => {2}\n'
                            'Start date & time => {3}\n'
                            'End date & time   => {4}\n'
                            'Status            => {5}'.format(
                                recording.channel_name,
                                recording.channel_number,
                                recording.program_title,
                                recording.start_date_time_in_utc.astimezone(
                                    get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                                recording.end_date_time_in_utc.astimezone(
                                    get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                                recording.status))

                        indices_of_recordings_to_delete.append(recording_index)

                for recording_index_to_delete in indices_of_recordings_to_delete:
                    del recordings[recording_index_to_delete]

                smooth_streams_proxy_db['recordings'] = recordings
            except KeyError:
                pass
            # </editor-fold>

    @classmethod
    def _clear_nimble_session_id_map(cls):
        with cls._nimble_session_id_map_lock:
            cls._nimble_session_id_map = {}

    @classmethod
    def _determine_server_hostname(cls, client_ip_address):
        ip_address_object = ipaddress.ip_address(client_ip_address)

        if ip_address_object.is_loopback:
            return cls.get_configuration_parameter('SERVER_HOSTNAME_LOOPBACK')
        elif ip_address_object in ipaddress.ip_network('10.0.0.0/8') or \
                ip_address_object in ipaddress.ip_network('172.16.0.0/12') or \
                ip_address_object in ipaddress.ip_network('192.168.0.0/16'):
            return cls.get_configuration_parameter('SERVER_HOSTNAME_PRIVATE')
        elif ip_address_object.is_global:
            return cls.get_configuration_parameter('SERVER_HOSTNAME_PUBLIC')

    @classmethod
    def _do_retrieve_authorization_hash(cls):
        try:
            if datetime.now(pytz.utc) < (cls._get_session_parameter('expires_on') - timedelta(minutes=30)):
                return False
            else:
                logger.info('Authorization hash\n'
                            'Status => Expired\n'
                            'Action => Retrieve it')

                return True
        except KeyError:
            logger.debug('Authorization hash\n'
                         'Status => Never retrieved\n'
                         'Action => Retrieve it')

            return True

    @classmethod
    def _download_file(cls, file_name):
        url = '{0}/{1}'.format(cls._get_epg_source_url(), file_name)

        logger.info('Downloading {0}\n'
                    'URL => {1}'.format(file_name, url))

        session = requests.Session()
        response = SmoothStreamsProxyUtility.make_http_request(session.get, url, headers=session.headers)

        response_status_code = response.status_code
        if response_status_code == requests.codes.OK:
            response_headers = response.headers
            response_content = response.content if file_name.endswith('.gz') else json.dumps(response.json(),
                                                                                             sort_keys=True,
                                                                                             indent=2)

            # noinspection PyUnresolvedReferences
            logger.trace(
                'Response from {0}\n'
                '[Status Code]\n'
                '=============\n{1}\n\n'
                '[Header]\n'
                '========\n{2}\n\n'
                '[Content]\n'
                '=========\n{3:,}\n'.format(url,
                                            response_status_code,
                                            '\n'.join(['{0:32} => {1!s}'.format(key, response_headers[key])
                                                       for key in sorted(response_headers)]),
                                            len(response_content)))

            if file_name.endswith('.gz'):
                try:
                    response_content = '<?xml version="1.0" encoding="UTF-8"?>{0}'.format(
                        gzip.decompress(response_content).decode('utf-8'))
                except OSError:
                    response_content = '<?xml version="1.0" encoding="UTF-8"?>{0}'.format(
                        response_content.decode('utf-8'))

            current_date_time_in_utc = datetime.now(pytz.utc)

            file_path = os.path.join(os.getcwd(), 'cache', '{0}_{1}'.format(
                current_date_time_in_utc.strftime('%Y%m%d%H%M%S'),
                file_name[:-3] if file_name.endswith('.gz') else file_name))
            with open(file_path, 'w') as out_file:
                out_file.write(response_content)

                logger.debug('{0} saved\n'
                             'File path => {1}'.format(file_name, file_path))

            if cls._is_file_name_in_file_map(file_name):
                try:
                    os.remove(cls._get_file_parameter(file_name, 'source'))
                except OSError:
                    (type_, value_, traceback_) = sys.exc_info()
                    logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

            file = {
                'size': len(response_content),
                'source': file_path,
                'next_update_date_time': current_date_time_in_utc.replace(microsecond=0,
                                                                          second=0,
                                                                          minute=0,
                                                                          hour=0) + timedelta(
                    hours=(((current_date_time_in_utc.hour // 4) * 4) + 3) +
                          ((current_date_time_in_utc.minute // 33) *
                           (4 if (current_date_time_in_utc.hour + 1) % 4 == 0 else 0)),
                    minutes=32)
            }

            cls._add_file_to_files_map(file_name, file)

            return response_content
        else:
            response.raise_for_status()

    @classmethod
    def _get_epg_source_url(cls):
        return cls._epg_source_urls[random.randint(0, len(cls._epg_source_urls) - 1)]

    @classmethod
    def _get_file_parameter(cls, file_name, parameter_name):
        with cls._files_map_lock:
            return cls._files_map[file_name][parameter_name]

    @classmethod
    def _get_scheduled_recordings(cls):
        with cls._recordings_lock:
            return [scheduled_recording
                    for scheduled_recording in cls._recordings
                    if scheduled_recording.status == SmoothStreamsProxyRecordingStatus.SCHEDULED.value]

    @classmethod
    def _get_session_parameter(cls, parameter_name):
        with cls._session_lock:
            return cls._session[parameter_name]

    @classmethod
    def _get_target_nimble_session_id(cls, hijacked_nimble_session_id):
        with cls._nimble_session_id_map_lock:
            return cls._nimble_session_id_map.get(hijacked_nimble_session_id, None)

    @classmethod
    def _is_file_name_in_file_map(cls, file_name):
        with cls._files_map_lock:
            return file_name in cls._files_map

    @classmethod
    def _hijack_nimble_session_id(cls, hijacked_nimble_session_id, hijacking_nimble_session_id):
        with cls._nimble_session_id_map_lock:
            cls._nimble_session_id_map[hijacked_nimble_session_id] = hijacking_nimble_session_id

    @classmethod
    def _load_shelved_settings(cls):
        shelf_file_path = cls._shelf_file_path
        logger.debug('Loading shelved settings\n'
                     'Shelf file => {0}'.format(shelf_file_path))

        with shelve.open(shelf_file_path) as smooth_streams_proxy_db:
            # <editor-fold desc="Load session">
            try:
                cls._session = smooth_streams_proxy_db['session']

                logger.debug('Loaded shelved session\n'
                             'Hash     => {0}\n'
                             'Valid to => {1}'.format(cls._get_session_parameter('hash'),
                                                      cls._get_session_parameter('expires_on').astimezone(
                                                          get_localzone()).strftime('%Y-%m-%d %H:%M:%S')))
            except KeyError:
                logger.debug('No shelved session exists\n'
                             'Shelf file path => {0}'.format(shelf_file_path))
            # </editor-fold>

            # <editor-fold desc="Load files_map">
            try:
                cls._files_map = smooth_streams_proxy_db['files_map']

                if cls._files_map:
                    logger.debug('Loaded shelved files\n{0}'.format(
                        '\n'.join(
                            ['File name => {0}\n'
                             'File size => {1:,}\n'
                             'Valid to  => {2}\n'.format(
                                file_name,
                                cls._get_file_parameter(file_name, 'size'),
                                cls._get_file_parameter(file_name, 'next_update_date_time').astimezone(
                                    get_localzone()).strftime('%Y-%m-%d %H:%M:%S'))
                                for file_name in sorted(cls._files_map)]).strip()))
            except KeyError:
                logger.debug('No shelved files exist\n'
                             'Shelf file path => {0}'.format(shelf_file_path))
            # </editor-fold>

            # <editor-fold desc="Load channel_map">
            try:
                cls._channel_map = smooth_streams_proxy_db['channel_map']

                logger.debug(
                    'Loaded shelved channel map\n'
                    '{0}'.format('\n'.join(['{0:03} => {1}'.format(channel_number, cls._channel_map[channel_number])
                                            for channel_number in sorted(cls._channel_map)])))
            except KeyError:
                logger.debug('No shelved channel map exists\n'
                             'Shelf file path => {0}'.format(shelf_file_path))
            # </editor-fold>

            # <editor-fold desc="Load fernet_key">
            try:
                cls._fernet_key = smooth_streams_proxy_db['fernet_key']

                logger.debug('Loaded shelved decryption key')
            except KeyError:
                logger.debug('No shelved decryption key exists\n'
                             'Shelf file path => {0}'.format(shelf_file_path))
            # </editor-fold>

            # <editor-fold desc="Load recordings">
            try:
                cls._recordings = smooth_streams_proxy_db['recordings']

                if cls._recordings:
                    logger.debug(
                        'Loaded shelved recordings\n{0}'.format(
                            '\n'.join([
                                'Channel name      => {0}\n'
                                'Channel number    => {1}\n'
                                'Program title     => {2}\n'
                                'Start date & time => {3}\n'
                                'End date & time   => {4}\n'
                                'Status            => {5}'.format(
                                    recording.channel_name,
                                    recording.channel_number,
                                    recording.program_title,
                                    recording.start_date_time_in_utc.astimezone(
                                        get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                                    recording.end_date_time_in_utc.astimezone(
                                        get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                                    recording.status)
                                for recording in cls._recordings]).strip()))
            except KeyError:
                logger.debug('No shelved recordings exist\n'
                             'Shelf file path => {0}'.format(shelf_file_path))
            # </editor-fold>

    @classmethod
    def _manage_password(cls):
        smooth_streams_password = cls.get_configuration_parameter('SMOOTH_STREAMS_PASSWORD')

        if SmoothStreamsProxyUtility.determine_password_state(smooth_streams_password) == \
                SmoothStreamsProxyPasswordState.DECRYPTED:
            cls._scrub_configuration_file()
        else:
            if cls._fernet_key:
                try:
                    SmoothStreamsProxyUtility.decrypt_password(cls._fernet_key, smooth_streams_password)

                    logger.debug('Decryption key loaded is valid for the encrypted SmoothStreams password')
                except InvalidToken:
                    logger.error(
                        'Decryption key loaded is not valid for the encrypted SmoothStreams password\n'
                        'Please re-enter your cleartext password in the configuration file\n'
                        'Configuration file path => {0}\n'
                        'Exiting...'.format(cls._configuration_file_path))

                    cls._remove_from_shelf('fernet_key')

                    sys.exit()
            else:
                logger.error(
                    'SmoothStreams password is encrypted, but no decryption key was found\n'
                    'Please re-enter your cleartext password in the configuration file\n'
                    'Configuration file path => {0}\n'
                    'Exiting...'.format(cls._configuration_file_path))

                sys.exit()

    @classmethod
    def _persist_to_shelf(cls, key, value):
        shelf_file_path = cls._shelf_file_path

        # noinspection PyUnresolvedReferences
        logger.trace('Persisting {0}\n'
                     'Shelf file path => {1}'.format(key, shelf_file_path))

        try:
            with shelve.open(shelf_file_path) as smooth_streams_proxy_db:
                smooth_streams_proxy_db[key] = value

                logger.debug('Persisted {0}\n'
                             'Shelf file path => {1}'.format(key, shelf_file_path))
        except OSError:
            logger.debug('Failed to persist {0}\n'
                         'Shelf file path => {1}'.format(key, shelf_file_path))

    @classmethod
    def _process_authorization_hash(cls, hash_response):
        if 'code' in hash_response:
            if hash_response['code'] == '0':
                logger.error('Failed to retrieved authorization token\n'
                             'Error => {0}'.format(hash_response['error']))
            elif hash_response['code'] == '1':
                cls._set_session_parameter('hash', hash_response['hash'])
                cls._set_session_parameter('expires_on',
                                           datetime.now(pytz.utc) + timedelta(seconds=(hash_response['valid'] * 60)))

                logger.info('Retrieved authorization token\n'
                            'Hash       => {0}\n'
                            'Expires On => {1}'.format(cls._get_session_parameter('hash'),
                                                       cls._get_session_parameter('expires_on').astimezone(
                                                           get_localzone()).strftime('%Y-%m-%d %H:%M:%S')[:-3]))

                cls._persist_to_shelf('session', cls._session)
        else:
            logger.error('Failed to retrieved authorization token\n'
                         'Error => JSON response contains no [\'code\'] field')

    @classmethod
    def _refresh_serviceable_clients(cls, client_uuid, client_ip_address):
        with cls._serviceable_clients_lock:
            if client_uuid not in cls._serviceable_clients:
                logger.debug('Adding client to serviceable clients\n'
                             'Client IP address => {0}\n'
                             'Client ID => {1}'.format(client_ip_address, client_uuid))

                cls._add_client_to_serviceable_clients(client_uuid, client_ip_address)
            else:
                cls._set_serviceable_client_parameter(client_uuid, 'last_request_date_time', datetime.now(pytz.utc))

    @classmethod
    def _refresh_session(cls, force_refresh=False):
        with cls._session_lock:
            do_start_timer = False

            if force_refresh or cls._do_retrieve_authorization_hash():
                do_start_timer = True

                cls._clear_nimble_session_id_map()

                cls._retrieve_authorization_hash()

                if cls._refresh_session_timer:
                    cls._refresh_session_timer.cancel()
            elif not cls._refresh_session_timer:
                do_start_timer = True

            if do_start_timer:
                interval = (cls._get_session_parameter('expires_on') - datetime.now(pytz.utc)).total_seconds() - 1800
                cls._refresh_session_timer = Timer(interval, cls._timed_refresh_session)
                cls._refresh_session_timer.start()

                logger.debug('Starting authorization hash refresh timer\n'
                             'Interval => {0} seconds'.format(interval))

    @classmethod
    def _remove_from_shelf(cls, key):
        shelf_file_path = cls._shelf_file_path

        # noinspection PyUnresolvedReferences
        logger.trace('Removing {0}\n'
                     'Shelf file path => {1}'.format(key, shelf_file_path))

        try:
            with shelve.open(shelf_file_path) as smooth_streams_proxy_db:
                del smooth_streams_proxy_db[key]

                logger.debug('Removed {0}\n'
                             'Shelf file path => {1}'.format(key, shelf_file_path))
        except IOError:
            logger.debug('Failed to remove {0}\n'
                         'Shelf file path => {1}'.format(key, shelf_file_path))

    @classmethod
    def _restart_active_recording(cls):
        with cls._active_recordings_to_recording_thread_lock, cls._recordings_lock:
            cls._set_active_recordings_to_recording_thread(
                {
                    recording.id: SmoothStreamsProxyRecordingThread(recording)
                    for recording in cls._recordings
                    if recording.status == SmoothStreamsProxyRecordingStatus.ACTIVE.value
                })

    @classmethod
    def _retrieve_authorization_hash(cls):

        cls._set_session_parameter('http_session', None)
        session = requests.Session()

        if cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE') == 'viewmmasr':
            url = 'https://www.mma-tv.net/loginForm.php'
        else:
            url = 'https://auth.smoothstreams.tv/hash_api.php'

        smooth_streams_username = cls.get_configuration_parameter('SMOOTH_STREAMS_USERNAME')
        smooth_streams_password = SmoothStreamsProxyUtility.decrypt_password(
            cls._fernet_key,
            cls.get_configuration_parameter('SMOOTH_STREAMS_PASSWORD')).decode()
        smooth_streams_site = cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE')

        logger.debug(
            'Retrieving authorization hash\n'
            'URL => {0}\n'
            '  Parameters\n'
            '    username => {0}\n'
            '    password => {1}\n'
            '    site     => {2}'.format(url,
                                         smooth_streams_username,
                                         smooth_streams_password,
                                         smooth_streams_site))

        response = SmoothStreamsProxyUtility.make_http_request(
            session.get,
            url,
            params={
                'username': smooth_streams_username,
                'password': smooth_streams_password,
                'site': smooth_streams_site
            },
            headers=session.headers,
            cookies=session.cookies.get_dict())

        response_status_code = response.status_code
        if response_status_code == requests.codes.OK:
            cls._set_session_parameter('http_session', session)
        elif response_status_code != requests.codes.NOT_FOUND:
            logger.error('Response from {0}\n'
                         '[Status Code]\n'
                         '=============\n{1}\n'.format(url, response_status_code))

            response.raise_for_status()

        response_headers = response.headers
        response_json = response.json()

        # noinspection PyUnresolvedReferences
        logger.trace(
            'Response from {0}\n'
            '[Status Code]\n'
            '=============\n{1}\n\n'
            '[Header]\n'
            '========\n{2}\n\n'
            '[Content]\n'
            '=========\n{3}\n'.format(url,
                                      response_status_code,
                                      '\n'.join(
                                          ['{0:32} => {1!s}'.format(key, response_headers[key])
                                           for key in sorted(response_headers)]),
                                      json.dumps(response_json, sort_keys=True, indent=2)))

        cls._process_authorization_hash(response_json)

        if response_status_code != requests.codes.OK:
            response.raise_for_status()

    @classmethod
    def _scrub_configuration_file(cls):
        encrypted_smooth_streams_password = SmoothStreamsProxyUtility.encrypt_password(
            cls._fernet_key,
            cls.get_configuration_parameter('SMOOTH_STREAMS_PASSWORD'))

        logger.debug('Scrubbed SmoothStreams password\n'
                     'Encrypted password => {0}'.format(encrypted_smooth_streams_password))

        cls._update_configuration_file('SmoothStreams', 'password', encrypted_smooth_streams_password)

        cls._set_configuration_parameter('SMOOTH_STREAMS_PASSWORD', encrypted_smooth_streams_password)

        cls._persist_to_shelf('fernet_key', cls._fernet_key)

    @classmethod
    def _set_active_recordings_to_recording_thread(cls, active_recordings_to_recording_thread):
        with cls._active_recordings_to_recording_thread_lock:
            cls._active_recordings_to_recording_thread = active_recordings_to_recording_thread

    @classmethod
    def _set_channel_map(cls, channel_map):
        with cls._channel_map_lock:
            cls._channel_map = channel_map

            cls._persist_to_shelf('channel_map', cls._channel_map)

    @classmethod
    def _set_configuration(cls, configuration):
        with cls._configuration_lock:
            cls._configuration = configuration

    @classmethod
    def _set_configuration_parameter(cls, parameter_name, parameter_value):
        with cls._configuration_lock:
            cls._configuration[parameter_name] = parameter_value

    @classmethod
    def _set_serviceable_client_parameter(cls, client_uuid, parameter_name, parameter_value):
        with cls._serviceable_clients_lock:
            cls._serviceable_clients[client_uuid][parameter_name] = parameter_value

    @classmethod
    def _set_session_parameter(cls, parameter_name, parameter_value):
        with cls._session_lock:
            cls._session[parameter_name] = parameter_value

    @classmethod
    def _set_start_recording_timer(cls):
        with cls._start_recording_timer_lock:
            if cls._start_recording_timer:
                cls._start_recording_timer.cancel()

            soonest_scheduled_recording_start_date_time_in_utc = None
            current_date_time_in_utc = datetime.now(pytz.utc)

            for scheduled_recording in cls._get_scheduled_recordings():
                scheduled_recording_start_date_time_in_utc = scheduled_recording.start_date_time_in_utc

                if current_date_time_in_utc > scheduled_recording_start_date_time_in_utc:
                    # Generate a new id for the recording when we change it's status
                    scheduled_recording.id = '{0}'.format(uuid.uuid4())
                    scheduled_recording.status = SmoothStreamsProxyRecordingStatus.ACTIVE.value

                    with cls._active_recordings_to_recording_thread_lock:
                        cls._active_recordings_to_recording_thread[
                            scheduled_recording.id] = SmoothStreamsProxyRecordingThread(scheduled_recording)
                elif not soonest_scheduled_recording_start_date_time_in_utc:
                    soonest_scheduled_recording_start_date_time_in_utc = scheduled_recording_start_date_time_in_utc
                elif soonest_scheduled_recording_start_date_time_in_utc > scheduled_recording_start_date_time_in_utc:
                    soonest_scheduled_recording_start_date_time_in_utc = scheduled_recording_start_date_time_in_utc

            if soonest_scheduled_recording_start_date_time_in_utc:
                interval = (soonest_scheduled_recording_start_date_time_in_utc - datetime.now(pytz.utc)).total_seconds()
                cls._start_recording_timer = Timer(interval, cls._start_recording)
                cls._start_recording_timer.start()

                logger.debug('Starting recording timer\n'
                             'Interval => {0} seconds'.format(interval))

    @classmethod
    def _shutdown_http_server(cls):
        for smooth_streams_proxy_http_request_handler_thread in cls._http_request_handler_threads:
            smooth_streams_proxy_http_request_handler_thread.stop()

            smooth_streams_proxy_http_request_handler_thread.join()

        cls._server_socket.close()

        logger.info('Shutdown HTTP Server\n'
                    'Listening port => {0}'.format(cls.get_configuration_parameter('SERVER_PORT')))

    @classmethod
    def _start_configuration_file_watchdog_observer(cls):
        smooth_streams_proxy_configuration_event_handler = SmoothStreamsProxyConfigurationEventHandler(
            cls._configuration_file_path)

        cls._configuration_file_watchdog_observer = Observer()
        cls._configuration_file_watchdog_observer.schedule(smooth_streams_proxy_configuration_event_handler,
                                                           os.path.dirname(cls._configuration_file_path),
                                                           recursive=False)
        cls._configuration_file_watchdog_observer.start()

    @classmethod
    def _start_http_server(cls):
        server_hostname_loopback = cls.get_configuration_parameter('SERVER_HOSTNAME_LOOPBACK')
        server_hostname_private = cls.get_configuration_parameter('SERVER_HOSTNAME_PRIVATE')
        server_hostname_public = cls.get_configuration_parameter('SERVER_HOSTNAME_PUBLIC')
        server_port = cls.get_configuration_parameter('SERVER_PORT')

        logger.info(
            'Starting HTTP Server\n'
            'Listening port             => {0}\n\n{1}\n'
            'Loopback Live Playlist URL => {2}\n'
            'Loopback VOD Playlist URL  => {3}\n'
            'Loopback EPG URL           => {4}\n\n'
            'Private Live Playlist URL  => {5}\n'
            'Private VOD Playlist URL   => {6}\n'
            'Private EPG URL            => {7}\n\n'
            'Public Live Playlist URL   => {8}\n'
            'Public VOD Playlist URL    => {9}\n'
            'Public EPG URL             => {10}\n'
            '{1}'.format(
                server_port,
                '=' * (max(
                    len(server_hostname_loopback),
                    len(server_hostname_private),
                    len(server_hostname_public)) + len('{0}'.format(server_port)) + 57),
                'http://{0}:{1}/live/playlist.m3u8'.format(server_hostname_loopback,
                                                           server_port),
                'http://{0}:{1}/vod/playlist.m3u8'.format(server_hostname_loopback,
                                                          server_port),
                'http://{0}:{1}/live/epg.xml'.format(server_hostname_loopback,
                                                     server_port),
                'http://{0}:{1}/live/playlist.m3u8'.format(server_hostname_private,
                                                           server_port) if server_hostname_private else 'N/A',
                'http://{0}:{1}/vod/playlist.m3u8'.format(server_hostname_private,
                                                          server_port) if server_hostname_private else 'N/A',
                'http://{0}:{1}/live/epg.xml'.format(server_hostname_private,
                                                     server_port) if server_hostname_private else 'N/A',
                'http://{0}:{1}/live/playlist.m3u8'.format(server_hostname_public,
                                                           server_port) if server_hostname_public else 'N/A',
                'http://{0}:{1}/vod/playlist.m3u8'.format(server_hostname_public,
                                                          server_port) if server_hostname_public else 'N/A',
                'http://{0}:{1}/live/epg.xml'.format(server_hostname_public,
                                                     server_port) if server_hostname_public else 'N/A'))

        server_address = ('', int(server_port))

        cls._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cls._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        cls._server_socket.bind(server_address)
        cls._server_socket.listen(5)

        cls._http_request_handler_threads = [SmoothStreamsProxyHTTPRequestHandlerThread(server_address,
                                                                                        cls._server_socket)
                                             for _ in range(cls._number_of_http_request_handler_threads)]

    @classmethod
    def _start_recording(cls):
        current_date_time_in_utc = datetime.now(pytz.utc)

        for scheduled_recording in cls._get_scheduled_recordings():
            scheduled_recording_start_date_time_in_utc = scheduled_recording.start_date_time_in_utc

            if current_date_time_in_utc > scheduled_recording_start_date_time_in_utc:
                # Generate a new id for the recording when we change it's status
                scheduled_recording.id = '{0}'.format(uuid.uuid4())
                scheduled_recording.status = SmoothStreamsProxyRecordingStatus.ACTIVE.value

                with cls._active_recordings_to_recording_thread_lock:
                    cls._active_recordings_to_recording_thread[
                        scheduled_recording.id] = SmoothStreamsProxyRecordingThread(scheduled_recording)

        cls._set_start_recording_timer()

    @classmethod
    def _stop_configuration_file_watchdog_observer(cls):
        cls._configuration_file_watchdog_observer.stop()

    @classmethod
    def _timed_refresh_session(cls):
        logger.debug('Authorization hash refresh timer triggered')

        cls._refresh_session(force_refresh=True)

    @classmethod
    def _update_configuration_file(cls, section, option, value):
        configuration_file_path = cls._configuration_file_path

        configuration = ConfigObj(configuration_file_path,
                                  file_error=True,
                                  interpolation=False,
                                  write_empty_values=True)
        configuration[section][option] = value

        try:
            configuration.write()

            logger.debug('Updated configuration file\n'
                         'Configuration file path => {0}\n\n'
                         'Section => {1}\n'
                         'Option  => {2}\n'
                         'Value   => {3}'.format(configuration_file_path, section, option, value))
        except OSError:
            logger.error('Could not open the specified configuration file for writing\n'
                         'Configuration file path => {0}'.format(configuration_file_path))

    @classmethod
    def add_scheduled_recording(cls, scheduled_recording):
        with cls._recordings_lock:
            if scheduled_recording not in cls._recordings:
                cls._recordings.append(scheduled_recording)

                cls._set_start_recording_timer()
            else:
                raise DuplicateRecordingError

    @classmethod
    def delete_active_recording(cls, active_recording):
        with cls._recordings_lock:
            cls._recordings.remove(active_recording)

            cls._persist_to_shelf('recordings', cls._recordings)

    @classmethod
    def delete_persisted_recording(cls, persisted_recording):
        shutil.rmtree(os.path.join(cls._recordings_directory_path, persisted_recording.base_recording_directory))

    @classmethod
    def delete_scheduled_recording(cls, scheduled_recording):
        with cls._recordings_lock:
            cls._recordings.remove(scheduled_recording)

            cls._persist_to_shelf('recordings', cls._recordings)

            cls._set_start_recording_timer()

    @classmethod
    def download_chunks_m3u8(cls, client_ip_address, requested_path, channel_number, client_uuid, nimble_session_id):
        cls._refresh_serviceable_clients(client_uuid, client_ip_address)

        smooth_streams_hash = cls._get_session_parameter('hash')
        smooth_streams_session = cls._get_session_parameter('http_session')

        target_url = 'https://{0}.smoothstreams.tv/{1}/ch{2}q1.stream{3}'.format(
            cls.get_configuration_parameter('SMOOTH_STREAMS_SERVER'),
            cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE'),
            channel_number,
            re.sub(r'(/.*)?(/.*\.m3u8)', r'\2', requested_path))

        logger.debug(
            'Proxying request\n'
            'Source IP      => {0}\n'
            'Requested path => {1}\n'
            '  Parameters\n'
            '    channel_number  => {2}\n'
            '    client_uuid     => {3}\n'
            'Target path    => {4}\n'
            '  Parameters\n'
            '    nimblesessionid => {5}\n'
            '    wmsAuthSign     => {6}'.format(
                client_ip_address,
                requested_path,
                channel_number,
                client_uuid,
                target_url,
                nimble_session_id,
                smooth_streams_hash))

        response = SmoothStreamsProxyUtility.make_http_request(smooth_streams_session.get,
                                                               target_url,
                                                               params={
                                                                   'nimblesessionid': nimble_session_id,
                                                                   'wmsAuthSign': smooth_streams_hash
                                                               },
                                                               headers=smooth_streams_session.headers,
                                                               cookies=smooth_streams_session.cookies.get_dict())

        response_status_code = response.status_code
        if response_status_code == requests.codes.OK:
            response_headers = response.headers
            response_text = response.text

            # noinspection PyUnresolvedReferences
            logger.trace(
                'Response from {0}\n'
                '[Status Code]\n'
                '=============\n{1}\n\n'
                '[Header]\n'
                '========\n{2}\n\n'
                '[Content]\n'
                '=========\n{3}\n'.format(target_url,
                                          response_status_code,
                                          '\n'.join(['{0:32} => {1!s}'.format(key, response_headers[key])
                                                     for key in sorted(response_headers)]),
                                          response_text))

            return response_text.replace('.ts?', '.ts?channel_number={0}&client_uuid={1}&'.format(channel_number,
                                                                                                  client_uuid))
        else:
            logger.error('Response from {0}\n'
                         '[Status Code]\n'
                         '=============\n{1}\n'.format(target_url, response_status_code))

            response.raise_for_status()

    @classmethod
    def download_playlist_m3u8(cls, client_ip_address, requested_path, channel_number, client_uuid, protocol):
        cls._refresh_serviceable_clients(client_uuid, client_ip_address)
        cls._refresh_session()

        if protocol == 'hls':
            smooth_streams_hash = cls._get_session_parameter('hash')
            smooth_streams_session = cls._get_session_parameter('http_session')

            target_url = 'https://{0}.smoothstreams.tv/{1}/ch{2}q1.stream{3}'.format(
                cls.get_configuration_parameter('SMOOTH_STREAMS_SERVER'),
                cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE'),
                channel_number,
                re.sub(r'(/.*)?(/.*\.m3u8)', r'\2', requested_path))

            logger.debug(
                'Proxying request\n'
                'Source IP      => {0}\n'
                'Requested path => {1}\n'
                '  Parameters\n'
                '    channel_number => {2}\n'
                '    client_uuid    => {3}\n'
                '    protocol       => {4}\n'
                'Target path    => {5}\n'
                '  Parameters\n'
                '    wmsAuthSign    => {6}'.format(
                    client_ip_address,
                    requested_path,
                    channel_number,
                    client_uuid,
                    protocol,
                    target_url,
                    smooth_streams_hash))

            response = SmoothStreamsProxyUtility.make_http_request(smooth_streams_session.get,
                                                                   target_url,
                                                                   params={
                                                                       'wmsAuthSign': smooth_streams_hash
                                                                   },
                                                                   headers=smooth_streams_session.headers,
                                                                   cookies=smooth_streams_session.cookies.get_dict())

            response_status_code = response.status_code
            if response_status_code == requests.codes.OK:
                response_headers = response.headers
                response_text = response.text

                # noinspection PyUnresolvedReferences
                logger.trace(
                    'Response from {0}\n'
                    '[Status Code]\n'
                    '=============\n{1}\n\n'
                    '[Header]\n'
                    '========\n{2}\n\n'
                    '[Content]\n'
                    '=========\n{3}\n'.format(target_url,
                                              response_status_code,
                                              '\n'.join(['{0:32} => {1!s}'.format(key,
                                                                                  response_headers[key])
                                                         for key in sorted(response_headers)]),
                                              response_text))

                return response_text.replace('chunks.m3u8?',
                                             'chunks.m3u8?channel_number={0}&client_uuid={1}&'.format(channel_number,
                                                                                                      client_uuid))
            else:
                logger.error('Response from {0}\n'
                             '[Status Code]\n'
                             '=============\n{1}\n'.format(target_url, response_status_code))

                response.raise_for_status()
        elif protocol == 'rtmp':
            smooth_streams_hash = cls._get_session_parameter('hash')

            return '#EXTM3U\n' \
                   '#EXTINF:-1 ,{0}\n' \
                   'rtmp://{1}.smoothstreams.tv:3635/{2}?' \
                   'wmsAuthSign={3}/ch{4}q1.stream'.format(cls.get_channel_name(int(channel_number)),
                                                           cls.get_configuration_parameter('SMOOTH_STREAMS_SERVER'),
                                                           cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE'),
                                                           smooth_streams_hash,
                                                           channel_number)

    @classmethod
    def download_ts_file(cls, client_ip_address, requested_path, channel_number, client_uuid, nimble_session_id):
        cls._refresh_serviceable_clients(client_uuid, client_ip_address)

        smooth_streams_hash = cls._get_session_parameter('hash')
        smooth_streams_session = cls._get_session_parameter('http_session')

        target_url = 'https://{0}.smoothstreams.tv/{1}/ch{2}q1.stream{3}'.format(
            cls.get_configuration_parameter('SMOOTH_STREAMS_SERVER'),
            cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE'),
            channel_number,
            re.sub(r'(/.*)?(/.*\.ts)', r'\2', requested_path))

        logger.debug(
            'Proxying request\n'
            'Source IP      => {0}\n'
            'Requested path => {1}\n'
            '  Parameters\n'
            '    channel_number  => {2}\n'
            '    client_uuid     => {3}\n'
            'Target path    => {4}\n'
            '  Parameters\n'
            '    nimblesessionid => {5}\n'
            '    wmsAuthSign     => {6}'.format(
                client_ip_address,
                requested_path,
                channel_number,
                client_uuid,
                target_url,
                nimble_session_id,
                smooth_streams_hash))

        response = SmoothStreamsProxyUtility.make_http_request(smooth_streams_session.get,
                                                               target_url,
                                                               params={
                                                                   'nimblesessionid': nimble_session_id,
                                                                   'wmsAuthSign': smooth_streams_hash
                                                               },
                                                               headers=smooth_streams_session.headers,
                                                               cookies=smooth_streams_session.cookies.get_dict())

        response_status_code = response.status_code
        if response_status_code == requests.codes.OK:
            response_headers = response.headers
            response_content = response.content

            # noinspection PyUnresolvedReferences
            logger.trace(
                'Response from {0}\n'
                '[Status Code]\n'
                '=============\n{1}\n\n'
                '[Header]\n'
                '========\n{2}\n\n'
                '[Content]\n'
                '=========\n{3:,}\n'.format(target_url,
                                            response_status_code,
                                            '\n'.join(['{0:32} => {1!s}'.format(key,
                                                                                response_headers[key])
                                                       for key in sorted(response_headers)]),
                                            len(response_content)))

            return response_content
        else:
            logger.error('Response from {0}\n'
                         '[Status Code]\n'
                         '=============\n{1}\n'.format(target_url, response_status_code))

            response.raise_for_status()

    @classmethod
    def generate_live_playlist_m3u8(cls, client_ip_address, channels_json, protocol):
        try:
            channel_map = {}
            playlist_m3u8 = []
            client_uuid = '{0}'.format(uuid.uuid4())

            server_hostname = cls._determine_server_hostname(client_ip_address)
            server_port = cls.get_configuration_parameter('SERVER_PORT')

            for channel_key in sorted(channels_json,
                                      key=lambda channel_key_: int(channels_json[channel_key_]['channum'])):
                channel = channels_json[channel_key]

                channel_number = channel['channum']
                channel_name = channel['channame']

                channel_map[int(channel_number)] = channel_name

                playlist_m3u8.append(
                    '#EXTINF:-1 group-title="{0}" '
                    'tvg-id="{1}" '
                    'tvg-name="{3}" '
                    'tvg-logo="http://speed.guide.smoothstreams.tv/assets/images/channels/{2}.png" '
                    'channel-id="{2}",{3}\n'.format(
                        'Live TV',
                        channel['xmltvid'],
                        channel_number,
                        channel_name))

                if protocol == 'hls':
                    playlist_m3u8.append(
                        'http://{0}:{1}/live/playlist.m3u8?channel_number={2:02}&client_uuid={3}&protocol={4}\n'.format(
                            server_hostname,
                            server_port,
                            int(channel_number),
                            client_uuid,
                            protocol))
                elif protocol == 'rtmp':
                    cls._refresh_session()

                    smooth_streams_hash = cls._get_session_parameter('hash')

                    playlist_m3u8.append(
                        'rtmp://{0}.smoothstreams.tv:3635/{1}?wmsAuthSign={2}/ch{3:02}q1.stream\n'.format(
                            cls.get_configuration_parameter('SMOOTH_STREAMS_SERVER'),
                            cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE'),
                            smooth_streams_hash,
                            int(channel_number),
                            client_uuid))

            cls._set_channel_map(channel_map)

            playlist_m3u8 = '#EXTM3U x-tvg-url="http://{0}:{1}/epg.xml"\n{2}'.format(
                server_hostname,
                server_port,
                ''.join(playlist_m3u8))

            logger.debug('Generated live playlist.m3u8')

            return playlist_m3u8
        except (KeyError, ValueError):
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

    @classmethod
    def generate_vod_playlist_m3u8(cls, client_ip_address):
        playlist_m3u8 = []
        client_uuid = '{0}'.format(uuid.uuid4())

        server_hostname = cls._determine_server_hostname(client_ip_address)
        server_port = cls.get_configuration_parameter('SERVER_PORT')

        for persistent_recording in cls.get_persistent_recordings():
            playlist_m3u8.append(
                '#EXTINF:-1,{0} - [{1} - {2}]\n'
                'http://{3}:{4}/vod/playlist.m3u8?client_uuid={5}&program_title={6}\n'.format(
                    persistent_recording.program_title,
                    persistent_recording.start_date_time_in_utc.astimezone(
                        get_localzone()).strftime('%Y-%m-%d %H:%M:%S%z'),
                    persistent_recording.end_date_time_in_utc.astimezone(
                        get_localzone()).strftime('%Y-%m-%d %H:%M:%S%z'),
                    server_hostname,
                    server_port,
                    client_uuid,
                    urllib.parse.quote(persistent_recording.base_recording_directory)))

        if playlist_m3u8:
            playlist_m3u8 = '#EXTM3U\n{0}'.format(''.join(playlist_m3u8))

            logger.debug('Generated VOD playlist.m3u8')
        else:
            logger.debug('No persistent recordings found. VOD playlist.m3u8 will not be generated')

        return playlist_m3u8

    @classmethod
    def get_channel_name(cls, channel_number):
        with cls._channel_map_lock:
            return cls._channel_map[channel_number] if channel_number in cls._channel_map else 'Channel {0:02}'.format(
                channel_number)

    @classmethod
    def get_configuration_parameter(cls, parameter_name):
        with cls._configuration_lock:
            return cls._configuration[parameter_name]

    @classmethod
    def get_file_content(cls, file_name):
        do_download_file = False

        with cls._files_map_lock:
            if file_name in cls._files_map:
                if datetime.now(pytz.utc) > cls._get_file_parameter(file_name, 'next_update_date_time'):
                    logger.debug('{0}\n'
                                 'Status => Stale\n'
                                 'Action => Download'.format(file_name))

                    do_download_file = True
                elif not os.path.exists(cls._get_file_parameter(file_name, 'source')):
                    logger.debug('{0}\n'
                                 'Status => Not found\n'
                                 'Action => Download'.format(file_name))

                    do_download_file = True
                else:
                    logger.debug('{0}\n'
                                 'Status => Current\n'
                                 'Action => Read'.format(file_name))

                    file_content = None
                    try:
                        with open(cls._get_file_parameter(file_name, 'source'), 'r') as in_file:
                            file_content = in_file.read()
                    except OSError:
                        do_download_file = True

                    if not do_download_file:
                        return file_content
            else:
                logger.debug('{0}\n'
                             'Status => Not found\n'
                             'Action => Download'.format(file_name))

                do_download_file = True

            if do_download_file:
                return cls._download_file(file_name)

    @classmethod
    def get_persistent_recordings(cls):
        persistent_recordings = []

        recordings_directory_path = cls._recordings_directory_path
        recordings_top_level_directory = [recording_top_level_directory
                                          for recording_top_level_directory in os.listdir(recordings_directory_path)
                                          if os.path.isdir(os.path.join(recordings_directory_path,
                                                                        recording_top_level_directory))]
        if recordings_top_level_directory:
            for recording_top_level_directory in recordings_top_level_directory:
                try:
                    recording_top_level_directory_path = os.path.join(recordings_directory_path,
                                                                      recording_top_level_directory,
                                                                      '.MANIFEST')
                    with open(recording_top_level_directory_path, 'r') as in_file:
                        recording_manifest = json.load(in_file)
                        if recording_manifest['status'] == 'Completed':
                            recording = SmoothStreamsProxyRecording(recording_manifest['channel_name'],
                                                                    recording_manifest['channel_number'],
                                                                    datetime.strptime(
                                                                        recording_manifest[
                                                                            'actual_end_date_time_in_utc'],
                                                                        '%Y-%m-%d %H:%M:%S%z'),
                                                                    recording_manifest['id'],
                                                                    recording_manifest['program_title'],
                                                                    datetime.strptime(
                                                                        recording_manifest[
                                                                            'actual_start_date_time_in_utc'],
                                                                        '%Y-%m-%d %H:%M:%S%z'),
                                                                    SmoothStreamsProxyRecordingStatus.PERSISTED.value)
                            recording.base_recording_directory = recording_manifest['base_recording_directory']
                            persistent_recordings.append(recording)
                except OSError:
                    logger.error('Failed to open .MANIFEST\n'
                                 '.MANIFEST file path => {0}'.format(os.path.join(recordings_directory_path,
                                                                                  recording_top_level_directory,
                                                                                  '.MANIFEST')))

        return persistent_recordings

    @classmethod
    def get_recording(cls, recording_id):
        with cls._recordings_lock:
            for recording in cls._recordings + cls.get_persistent_recordings():
                if recording.id == recording_id:
                    return recording

            raise RecordingNotFoundError

    @classmethod
    def get_recordings(cls):
        with cls._recordings_lock:
            return cls._recordings + cls.get_persistent_recordings()

    @classmethod
    def get_recordings_directory_path(cls):
        return cls._recordings_directory_path

    @classmethod
    def get_serviceable_client_parameter(cls, client_uuid, parameter_name):
        with cls._serviceable_clients_lock:
            return cls._serviceable_clients[client_uuid][parameter_name]

    @classmethod
    def is_channel_number_in_channel_map(cls, channel_number):
        with cls._channel_map_lock:
            return int(channel_number) in cls._channel_map

    @classmethod
    def map_nimble_session_id(cls, client_ip_address, requested_path, channel_number, client_uuid, nimble_session_id,
                              smooth_streams_hash):
        if smooth_streams_hash != cls._get_session_parameter('hash'):
            target_nimble_session_id = cls._get_target_nimble_session_id(nimble_session_id)

            if not target_nimble_session_id:
                logger.debug('Authorization hash {0} in request from {1}/{2} expired'.format(smooth_streams_hash,
                                                                                             client_ip_address,
                                                                                             client_uuid))

                try:
                    response_text = cls.download_playlist_m3u8(client_ip_address,
                                                               requested_path,
                                                               channel_number,
                                                               client_uuid,
                                                               'hls')

                    m3u8_obj = m3u8.loads(response_text)

                    requested_path_with_query_string = '/{0}'.format(m3u8_obj.data['playlists'][0]['uri'])
                    requested_url_components = urllib.parse.urlparse(requested_path_with_query_string)
                    requested_query_string_parameters = dict(urllib.parse.parse_qsl(requested_url_components.query))

                    target_nimble_session_id = requested_query_string_parameters.get('nimblesessionid',
                                                                                     nimble_session_id)

                    logger.debug('Hijacking session\n'
                                 'Expired nimble session ID => {0}\n'
                                 'Target nimble session ID  => {1}'.format(nimble_session_id, target_nimble_session_id))
                    cls._hijack_nimble_session_id(nimble_session_id, target_nimble_session_id)
                except requests.exceptions.HTTPError:
                    target_nimble_session_id = nimble_session_id

                    (type_, value_, traceback_) = sys.exc_info()
                    logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))
        else:
            target_nimble_session_id = nimble_session_id

        return target_nimble_session_id

    @classmethod
    def process_configuration_file_updates(cls):
        refresh_session = False

        if cls.get_configuration_parameter('SERVER_PORT') != cls._previous_configuration['SERVER_PORT']:
            cls._shutdown_http_server()
            cls._start_http_server()

        if cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE') != \
                cls._previous_configuration['SMOOTH_STREAMS_SERVICE']:
            refresh_session = True

        if cls.get_configuration_parameter('SMOOTH_STREAMS_PASSWORD') != \
                cls._previous_configuration['SMOOTH_STREAMS_PASSWORD']:
            # Disable the configuration file watchdog to avoid processing of the event that results from scrubbing the 
            # password in the configuration file
            cls._stop_configuration_file_watchdog_observer()
            cls._manage_password()
            cls._start_configuration_file_watchdog_observer()

            refresh_session = True

        if cls.get_configuration_parameter('SMOOTH_STREAMS_USERNAME') != \
                cls._previous_configuration['SMOOTH_STREAMS_USERNAME']:
            refresh_session = True

        if cls.get_configuration_parameter('LOGGING_LEVEL') != cls._previous_configuration['LOGGING_LEVEL']:
            try:
                SmoothStreamsProxyUtility.set_logging_level(
                    getattr(logging, SmoothStreamsProxy.get_configuration_parameter('LOGGING_LEVEL').upper()))
            except AttributeError:
                (type_, value_, traceback_) = sys.exc_info()
                logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

        if refresh_session:
            cls._refresh_session(force_refresh=True)

    @classmethod
    def read_configuration_file(cls, initial_read=True):
        cls._backup_configuration()

        try:
            configuration = ConfigObj(cls._configuration_file_path,
                                      file_error=True,
                                      indent_type='',
                                      interpolation=False,
                                      raise_errors=True,
                                      write_empty_values=True)

            non_defaultable_error_in_configuration_file = False
            error_messages = []

            server_hostname_loopback = DEFAULT_HOSTNAME_LOOPBACK
            server_hostname_private = None
            server_hostname_public = None
            server_port = None
            smooth_streams_service = None
            smooth_streams_server = None
            smooth_streams_username = None
            smooth_streams_password = None
            smooth_streams_protocol = None
            logging_level = DEFAULT_LOGGING_LEVEL

            # <editor-fold desc="Read Server section">
            try:
                server_section = configuration['Server']

                try:
                    server_hostnames_section = server_section['Hostnames']

                    # <editor-fold desc="Read loopback option">
                    try:
                        server_hostname_loopback = server_hostnames_section['loopback']

                        ip_address_object = ipaddress.ip_address(server_hostname_loopback)
                        if not ip_address_object.is_loopback:
                            server_hostname_loopback = DEFAULT_HOSTNAME_LOOPBACK

                            error_messages.append('The loopback option within the [Hostnames] section specifies an '
                                                  'invalid loopback IP address\n'
                                                  'Defaulting to {0}\n'.format(server_hostname_loopback))
                    except KeyError:
                        server_hostname_loopback = DEFAULT_HOSTNAME_LOOPBACK

                        error_messages.append('The loopback option within the [Hostnames] section is missing\n'
                                              'Defaulting to {0}\n'.format(server_hostname_loopback))
                    except ValueError:
                        if server_hostname_loopback.lower() != DEFAULT_HOSTNAME_LOOPBACK:
                            server_hostname_loopback = DEFAULT_HOSTNAME_LOOPBACK

                            error_messages.append('The loopback option within the [Hostnames] section specifies an '
                                                  'invalid loopback hostname\n'
                                                  'Defaulting to {0}\n'.format(server_hostname_loopback))
                    # </editor-fold>

                    # <editor-fold desc="Read private option">
                    do_determine_private_ip_address = False

                    try:
                        server_hostname_private = server_hostnames_section['private']

                        ip_address_object = ipaddress.ip_address(server_hostname_private)
                        if ip_address_object not in ipaddress.ip_network('10.0.0.0/8') and \
                                ip_address_object not in ipaddress.ip_network('172.16.0.0/12') and \
                                ip_address_object not in ipaddress.ip_network('192.168.0.0/16'):
                            if ipaddress.ip_address(server_hostname_private).is_global:
                                error_messages.append(
                                    'The private option within the [Hostnames] section specifies a public IP address\n')
                            else:
                                do_determine_private_ip_address = True
                    except KeyError:
                        do_determine_private_ip_address = True
                    except ValueError:
                        # This is a weak attempt to differentiate between a badly input IP address and a hostname.
                        if re.match('\A[0-9]+\.[0-9]+.[0-9]+.[0-9]+\Z', server_hostname_private) \
                                or not SmoothStreamsProxyUtility.is_valid_hostname(server_hostname_private):
                            do_determine_private_ip_address = True
                    # </editor-fold>

                    # <editor-fold desc="Read public option">
                    do_determine_public_ip_address = False

                    try:
                        server_hostname_public = server_hostnames_section['public']
                        if not ipaddress.ip_address(server_hostname_public).is_global:
                            do_determine_public_ip_address = True
                    except KeyError:
                        do_determine_public_ip_address = True
                    except ValueError:
                        # This is a weak attempt to differentiate between a badly input IP address and a hostname.
                        if re.match('\A[0-9]+\.[0-9]+.[0-9]+.[0-9]+\Z', server_hostname_public) \
                                or not SmoothStreamsProxyUtility.is_valid_hostname(server_hostname_public):
                            do_determine_public_ip_address = True
                    # </editor-fold>
                except KeyError:
                    server_hostname_loopback = DEFAULT_HOSTNAME_LOOPBACK

                    do_determine_private_ip_address = True
                    do_determine_public_ip_address = True

                if do_determine_private_ip_address:
                    server_hostname_private = SmoothStreamsProxyUtility.determine_private_ip_address()

                    if server_hostname_private:
                        error_messages.append('The private option within the [Hostnames] section specifies an invalid '
                                              'private IP address\n'
                                              'Reverting to {0}\n'.format(server_hostname_private))

                if do_determine_public_ip_address:
                    server_hostname_public = SmoothStreamsProxyUtility.determine_public_ip_address()

                    if server_hostname_public:
                        error_messages.append('The public option within the [Hostnames] section specifies an invalid '
                                              'public IP address\n'
                                              'Reverting to {0}\n'.format(server_hostname_public))

                try:
                    server_port = server_section['port']
                    port = int(server_port)
                    if port < 0 or port > 65535:
                        non_defaultable_error_in_configuration_file = True

                        error_messages.append(
                            'The port option within the [Server] section must be a number between 0 and 65535\n')
                except KeyError:
                    non_defaultable_error_in_configuration_file = True

                    error_messages.append(
                        'Could not find a port option within the [Server] section\n'
                        'The port option within the [Server] section must be a number between 0 and 65535\n')
                except ValueError:
                    non_defaultable_error_in_configuration_file = True

                    error_messages.append(
                        'The port option within the [Server] section must be a number between 0 and 65535\n')
            except KeyError:
                non_defaultable_error_in_configuration_file = True

                error_messages.append('Could not find a [Server] section\n')
            # </editor-fold>

            # <editor-fold desc="Read SmoothStreams section">
            try:
                smooth_streams_section = configuration['SmoothStreams']

                try:
                    smooth_streams_service = smooth_streams_section['service']
                    if smooth_streams_service.lower() not in VALID_SMOOTH_STREAMS_SERVICE_VALUES:
                        non_defaultable_error_in_configuration_file = True

                        error_messages.append(
                            'The service option within the [SmoothStreams] section must be one of\n{0}\n'.format(
                                '\n'.join(['\u2022 {0}'.format(service)
                                           for service in VALID_SMOOTH_STREAMS_SERVICE_VALUES])))
                except KeyError:
                    non_defaultable_error_in_configuration_file = True

                    error_messages.append(
                        'Could not find a service option within the [SmoothStreams] section\n'
                        'The service option within the [SmoothStreams] section must be one of\n{0}\n'.format(
                            '\n'.join(['\u2022 {0}'.format(service)
                                       for service in VALID_SMOOTH_STREAMS_SERVICE_VALUES])))

                try:
                    smooth_streams_server = smooth_streams_section['server']
                    if smooth_streams_server.lower() not in VALID_SMOOTH_STREAMS_SERVER_VALUES:
                        non_defaultable_error_in_configuration_file = True

                        error_messages.append(
                            'The server option within the [SmoothStreams] section must be one of\n{0}\n'.format(
                                '\n'.join(['\u2022 {0}'.format(service)
                                           for service in VALID_SMOOTH_STREAMS_SERVER_VALUES])))
                except KeyError:
                    non_defaultable_error_in_configuration_file = True

                    error_messages.append(
                        'Could not find a server option within the [SmoothStreams] section\n'
                        'The server option within the [SmoothStreams] section must be one of\n{0}\n'.format(
                            '\n'.join(['\u2022 {0}'.format(service)
                                       for service in VALID_SMOOTH_STREAMS_SERVER_VALUES])))

                try:
                    smooth_streams_username = smooth_streams_section['username']
                except KeyError:
                    non_defaultable_error_in_configuration_file = True

                    error_messages.append('Could not find a username option within the [SmoothStreams] section\n')

                try:
                    smooth_streams_password = smooth_streams_section['password']
                except KeyError:
                    non_defaultable_error_in_configuration_file = True

                    error_messages.append('Could not find a password option within the [SmoothStreams] section\n')

                try:
                    smooth_streams_protocol = smooth_streams_section['protocol']
                    if smooth_streams_protocol.lower() not in VALID_SMOOTH_STREAMS_PROTOCOL_VALUES:
                        non_defaultable_error_in_configuration_file = True

                        error_messages.append(
                            'The protocol option within the [SmoothStreams] section must be one of\n{0}\n'.format(
                                '\n'.join(['\u2022 {0}'.format(service)
                                           for service in VALID_SMOOTH_STREAMS_PROTOCOL_VALUES])))
                except KeyError:
                    non_defaultable_error_in_configuration_file = True

                    error_messages.append(
                        'Could not find a protocol option within the [SmoothStreams] section\n'
                        'The protocol option within the [SmoothStreams] section must be one of\n{0}\n'.format(
                            '\n'.join(['\u2022 {0}'.format(service)
                                       for service in VALID_SMOOTH_STREAMS_PROTOCOL_VALUES])))
            except KeyError:
                non_defaultable_error_in_configuration_file = True

                error_messages.append('Could not find a [SmoothStreams] section\n')
            # </editor-fold>

            # <editor-fold desc="Read Logging section">
            try:
                logging_section = configuration['Logging']

                logging_level = logging_section['level']
                if logging_level.upper() not in VALID_LOGGING_LEVEL_VALUES:
                    logging_level = DEFAULT_LOGGING_LEVEL
            except KeyError:
                pass
            # </editor-fold>

            if error_messages:
                error_messages.insert(0,
                                      '{0} configuration file values\n'
                                      'Configuration file path => {1}\n'.format(
                                          'Invalid' if non_defaultable_error_in_configuration_file
                                          else 'Warnings regarding',
                                          cls._configuration_file_path))

                if initial_read and non_defaultable_error_in_configuration_file:
                    error_messages.append('Exiting...')
                elif non_defaultable_error_in_configuration_file:
                    error_messages.append('Skipping...')
                else:
                    error_messages.append('Processing with default values...')

                logger.error('\n'.join(error_messages))

                if initial_read and non_defaultable_error_in_configuration_file:
                    sys.exit()

            if not non_defaultable_error_in_configuration_file:
                configuration = {
                    'SERVER_HOSTNAME_LOOPBACK': server_hostname_loopback,
                    'SERVER_HOSTNAME_PRIVATE': server_hostname_private,
                    'SERVER_HOSTNAME_PUBLIC': server_hostname_public,
                    'SERVER_PORT': server_port,
                    'SMOOTH_STREAMS_SERVICE': smooth_streams_service,
                    'SMOOTH_STREAMS_SERVER': smooth_streams_server,
                    'SMOOTH_STREAMS_USERNAME': smooth_streams_username,
                    'SMOOTH_STREAMS_PASSWORD': smooth_streams_password,
                    'SMOOTH_STREAMS_PROTOCOL': smooth_streams_protocol,
                    'LOGGING_LEVEL': logging_level
                }
                cls._set_configuration(configuration)

                logger.info('{0}ead configuration file\n'
                            'Configuration file path  => {1}\n\n'
                            'SERVER_HOSTNAME_LOOPBACK => {2}\n'
                            'SERVER_HOSTNAME_PRIVATE  => {3}\n'
                            'SERVER_HOSTNAME_PUBLIC   => {4}\n'
                            'SERVER_PORT              => {5}\n'
                            'SMOOTH_STREAMS_SERVICE   => {6}\n'
                            'SMOOTH_STREAMS_SERVER    => {7}\n'
                            'SMOOTH_STREAMS_USERNAME  => {8}\n'
                            'SMOOTH_STREAMS_PASSWORD  => {9}\n'
                            'SMOOTH_STREAMS_PROTOCOL  => {10}\n'
                            'LOGGING_LEVEL            => {11}'.format('R' if initial_read else 'Rer',
                                                                      cls._configuration_file_path,
                                                                      server_hostname_loopback,
                                                                      server_hostname_private,
                                                                      server_hostname_public,
                                                                      server_port,
                                                                      smooth_streams_service,
                                                                      smooth_streams_server,
                                                                      smooth_streams_username,
                                                                      smooth_streams_password,
                                                                      smooth_streams_protocol,
                                                                      logging_level))
        except OSError:
            logger.error('Could not open the specified configuration file for reading\n'
                         'Configuration file path => {0}'
                         '{1}'.format(cls._configuration_file_path,
                                      '\n\nExiting...' if initial_read else ''))

            if initial_read:
                sys.exit()
        except SyntaxError as e:
            logger.error('Invalid configuration file syntax\n'
                         'Configuration file path => {0}\n'
                         '{1}'
                         '{2}'.format(cls._configuration_file_path,
                                      '{0}'.format(e),
                                      '\n\nExiting...' if initial_read else ''))

            if initial_read:
                sys.exit()

    @classmethod
    def read_ts_file(cls, path, program_title):
        ts_file_content = None
        ts_file_path = os.path.join(cls._recordings_directory_path,
                                    program_title,
                                    'segments',
                                    re.sub(r'/vod/(.*)\?.*', r'\1', path))

        try:
            with open(ts_file_path, 'rb') as in_file:
                ts_file_content = in_file.read()

                # noinspection PyUnresolvedReferences
                logger.trace('Read segment\n'
                             'Segment file path => {0}'.format(ts_file_path))
        except OSError:
            logger.error('Failed to read segment\n'
                         'Segment file path => {0}'.format(ts_file_path))

        return ts_file_content

    @classmethod
    def read_vod_playlist_m3u8(cls, program_title):
        vod_playlist_m3u8_content = None
        vod_playlist_m3u8_file_path = os.path.join(cls._recordings_directory_path,
                                                   program_title,
                                                   'playlist',
                                                   'playlist.m3u8')

        try:
            with open(vod_playlist_m3u8_file_path, 'r') as in_file:
                vod_playlist_m3u8_content = in_file.read()

                # noinspection PyUnresolvedReferences
                logger.trace('Read playlist\n'
                             'Playlist file path => {0}'.format(vod_playlist_m3u8_file_path))
        except OSError:
            logger.error('Failed to read playlist\n'
                         'Playlist file path => {0}'.format(vod_playlist_m3u8_file_path))

        return vod_playlist_m3u8_content

    @classmethod
    def set_serviceable_client_parameter(cls, client_uuid, parameter_name, parameter_value):
        with cls._serviceable_clients_lock:
            cls._serviceable_clients[client_uuid][parameter_name] = parameter_value

    @classmethod
    def shutdown_proxy(cls):
        cls._shutdown_proxy_event.set()

        if cls._refresh_session_timer:
            cls._refresh_session_timer.cancel()

        if cls._start_recording_timer:
            cls._start_recording_timer.cancel()

        for smooth_streams_proxy_http_request_handler_thread in cls._http_request_handler_threads:
            smooth_streams_proxy_http_request_handler_thread.stop()

        cls._stop_configuration_file_watchdog_observer()

    @classmethod
    def start_proxy(cls, configuration_file_path, log_file_path, recordings_directory_path, shelf_file_path,
                    number_of_http_request_handler_threads):
        cls._configuration_file_path = configuration_file_path
        cls._log_file_path = log_file_path
        cls._recordings_directory_path = recordings_directory_path
        cls._shelf_file_path = shelf_file_path

        cls.read_configuration_file()
        try:
            SmoothStreamsProxyUtility.set_logging_level(
                getattr(logging, cls.get_configuration_parameter('LOGGING_LEVEL').upper()))
        except AttributeError:
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

        cls._cleanup_shelf()
        cls._load_shelved_settings()
        cls._manage_password()
        cls._refresh_session()
        cls._restart_active_recording()
        cls._set_start_recording_timer()
        cls._start_configuration_file_watchdog_observer()

        cls._number_of_http_request_handler_threads = number_of_http_request_handler_threads
        cls._start_http_server()

        while not cls._shutdown_proxy_event.is_set():
            index = 0
            while index < len(cls._http_request_handler_threads):
                smooth_streams_proxy_http_request_handler_thread = cls._http_request_handler_threads[index]
                smooth_streams_proxy_http_request_handler_thread.join()

                del cls._http_request_handler_threads[index]

            cls._shutdown_proxy_event.wait(0.5)

        cls._configuration_file_watchdog_observer.join()

    @classmethod
    def stop_active_recording(cls, active_recording):
        with cls._active_recordings_to_recording_thread_lock:
            cls._active_recordings_to_recording_thread[active_recording.id].force_stop()


class SmoothStreamsProxyConfigurationEventHandler(FileSystemEventHandler):
    def __init__(self, configuration_file_path):
        FileSystemEventHandler.__init__(self)

        self._configuration_file_path = configuration_file_path
        self._last_modification_date_time = None
        self._event_handler_lock = RLock()

    def on_modified(self, event):
        modification_event_date_time_in_utc = datetime.now(pytz.utc)
        do_read_configuration_file = False

        with self._event_handler_lock:
            if event.src_path == self._configuration_file_path:
                # Read the configuration file if this is the first modification since the proxy started or if the
                # modification events are at least 1s apart (A hack to deal with watchdog generating duplicate events)
                if not self._last_modification_date_time:
                    do_read_configuration_file = True

                    self._last_modification_date_time = modification_event_date_time_in_utc
                else:
                    total_time_between_modifications = \
                        (modification_event_date_time_in_utc - self._last_modification_date_time).total_seconds()

                    if total_time_between_modifications >= 1.0:
                        do_read_configuration_file = True

                        self._last_modification_date_time = \
                            modification_event_date_time_in_utc

                if do_read_configuration_file:
                    SmoothStreamsProxy.read_configuration_file(initial_read=False)
                    SmoothStreamsProxy.process_configuration_file_updates()


def trace(self, msg, *args, **kwargs):
    if self.isEnabledFor(TRACE):
        self._log(TRACE, msg, args, **kwargs)


def main():
    (configuration_file_path,
     log_file_path,
     recordings_directory_path,
     shelf_file_path) = SmoothStreamsProxyUtility.parse_command_line_arguments()

    configuration_file_path = os.path.abspath(configuration_file_path)
    log_file_path = os.path.abspath(log_file_path)
    recordings_directory_path = os.path.abspath(recordings_directory_path)
    shelf_file_path = os.path.abspath(shelf_file_path)

    SmoothStreamsProxyUtility.initialize_logging(log_file_path)

    logger.info('Starting SmoothStreams Proxy {0}'.format(VERSION))

    SmoothStreamsProxy.start_proxy(configuration_file_path,
                                   log_file_path,
                                   recordings_directory_path,
                                   shelf_file_path,
                                   5)

    logger.info('Shutting down SmoothStreams Proxy {0}'.format(VERSION))


from .http_server import SmoothStreamsProxyHTTPRequestHandlerThread
from .recorder import SmoothStreamsProxyRecording
from .recorder import SmoothStreamsProxyRecordingThread
