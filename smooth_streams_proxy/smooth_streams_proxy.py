import argparse
import base64
import binascii
import configparser
import gzip
import json
import logging.handlers
import math
import os
import pprint
import random
import re
import shelve
import socket
import sys
import threading
import time
import traceback
import urllib.parse
import uuid
from datetime import datetime
from datetime import timedelta
from enum import Enum
from http.server import BaseHTTPRequestHandler
from http.server import HTTPServer

import m3u8
import pytz
import requests
from cryptography.fernet import Fernet
from cryptography.fernet import InvalidToken
from tzlocal import get_localzone
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .exceptions import DuplicateRecordingError

TRACE = 5
VALID_LOGGING_LEVEL_VALUES = ['DEBUG', 'ERROR', 'INFO', 'TRACE']
VALID_SMOOTH_STREAMS_PROTOCOL_VALUES = ['hls', 'rtmp']
VALID_SMOOTH_STREAMS_SERVER_VALUES = ['dap', 'deu', 'deu-de', 'deu-nl', 'deu-nl1', 'deu-nl2', 'deu-nl3', 'deu-nl4',
                                      'deu-nl5', 'deu-uk', 'deu-uk1', 'deu-uk2', 'dna', 'dnae', 'dnae1', 'dnae2',
                                      'dnae3', 'dnae4', 'dnae6', 'dnaw', 'dnaw1', 'dnaw2', 'dnaw3', 'dnaw4x'
                                      ]
VALID_SMOOTH_STREAMS_SERVICE_VALUES = ['view247', 'viewmmasr', 'viewss', 'viewstvn']
VERSION = '2.0.0'

logger = logging.getLogger(__name__)


class MultiLineFormatter(logging.Formatter):
    def format(self, record):
        formatted_string = logging.Formatter.format(self, record)
        (header, footer) = formatted_string.split(record.message)
        formatted_string = formatted_string.replace('\n', '\n' + ' ' * len(header))
        return formatted_string


class PasswordState(Enum):
    DECRYPTED = 0
    ENCRYPTED = 1


class Recording():
    __slots__ = ['_base_recording_directory', '_channel_number', '_end_date_time_in_utc', '_program_title',
                 '_start_date_time_in_utc']

    def __init__(self, channel_number, end_date_time_in_utc, program_title, start_date_time_in_utc):
        self._base_recording_directory = None
        self._channel_number = channel_number
        self._end_date_time_in_utc = end_date_time_in_utc
        self._program_title = program_title
        self._start_date_time_in_utc = start_date_time_in_utc

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self._channel_number, self._end_date_time_in_utc, self._program_title,
                    self._start_date_time_in_utc) == (
                       other._channel_number, other._end_date_time_in_utc, other._program_title,
                       other._start_date_time_in_utc)
        return False

    def __repr__(self):
        return '{0}('.format(self.__class__.__name__) + ', '.join(
            ['{0}={1!r}'.format(attribute_name[1:], getattr(self, attribute_name)) for attribute_name in
             self.__slots__]) + ')'

    def __str__(self):
        return '{0}('.format(self.__class__.__name__) + ', '.join(
            ['{0}={1!s}'.format(attribute_name, getattr(self, attribute_name)) for attribute_name in
             self.__slots__]) + ')'

    @property
    def base_recording_directory(self):
        return self._base_recording_directory

    @base_recording_directory.setter
    def base_recording_directory(self, base_recording_directory):
        self._base_recording_directory = base_recording_directory

    @property
    def channel_number(self):
        return self._channel_number

    @property
    def end_date_time_in_utc(self):
        return self._end_date_time_in_utc

    @property
    def program_title(self):
        return self._program_title

    @property
    def start_date_time_in_utc(self):
        return self._start_date_time_in_utc


class SmoothStreamsProxyRecordingThread(threading.Thread):
    def __init__(self, recording):
        threading.Thread.__init__(self)

        self._id = uuid.uuid4()
        self._recording = recording
        self._recording_directory_path = None

        self._stop_recording_event = threading.Event()
        self._stop_recording_timer = threading.Timer(
            (self._recording.end_date_time_in_utc - datetime.now(pytz.utc)).total_seconds(),
            self._set_stop_recording_event)
        self._stop_recording_timer.start()

        self.start()

    def _create_recording_directory_tree(self):
        recording_directory_suffix_counter = 0
        recording_directory_suffix = ''

        did_make_directory = False
        while not did_make_directory:
            # base64.urlsafe_b64encode() the base directory. This results in a valid directory name on any OS at the
            # expense of human readability.
            recording_directory_path = os.path.join(SmoothStreamsProxy.get_recordings_directory_path(),
                                                    base64.urlsafe_b64encode('{0}{1}'.format(
                                                        self._recording.program_title,
                                                        recording_directory_suffix).encode()).decode())
            if os.path.exists(recording_directory_path):
                recording_directory_suffix_counter += 1
                recording_directory_suffix = '_{0}'.format(recording_directory_suffix_counter)
            else:
                logger.debug('Creating recording directory tree for {0}\nPath => {1}'.format(
                    self._recording.program_title, recording_directory_path))

                try:
                    os.makedirs(recording_directory_path)
                    os.makedirs(os.path.join(recording_directory_path, 'playlist'))
                    os.makedirs(os.path.join(recording_directory_path, 'segments'))

                    did_make_directory = True
                    self._recording_directory_path = recording_directory_path
                    self._recording.base_recording_directory = os.path.split(recording_directory_path)[-1]

                    logger.debug('Created recording directory tree for {0}\nPath => {1}'.format(
                        self._recording.program_title, recording_directory_path))
                except OSError:
                    logger.error(
                        'Failed to create recording directory tree for {0}\nPath => {1}'.format(
                            self._recording.program_title, recording_directory_path))

                    recording_directory_suffix_counter += 1
                    recording_directory_suffix = '_{0}'.format(recording_directory_suffix_counter)

    def _save_manifest_file(self, actual_end_date_time_in_utc, actual_start_date_time_in_utc, playlist_file, status):
        manifest_file_path = os.path.join(self._recording_directory_path, '.MANIFEST')

        try:
            with open(manifest_file_path, 'w') as out_file:
                logger.info(json.dumps(actual_start_date_time_in_utc))
                json.dump(
                    {'actual_end_date_time_in_utc': actual_end_date_time_in_utc,
                     'actual_start_date_time_in_utc': actual_start_date_time_in_utc,
                     'channel_name': SmoothStreamsProxy.get_channel_name(int(self._recording.channel_number)),
                     'base_recording_directory': self._recording.base_recording_directory,
                     'channel_number': self._recording.channel_number,
                     'playlist_directory': os.path.join(self._recording_directory_path, 'playlist'),
                     'playlist_file': playlist_file,
                     'program_title': self._recording.program_title,
                     'segments_directory': os.path.join(self._recording_directory_path, 'segments'),
                     'scheduled_end_date_time_in_utc': self._recording.end_date_time_in_utc.strftime(
                         '%Y-%m-%d %H:%M:%S%z'),
                     'scheduled_start_date_time_in_utc': self._recording.start_date_time_in_utc.strftime(
                         '%Y-%m-%d %H:%M:%S%z'),
                     'status': status},
                    out_file,
                    sort_keys=True,
                    indent=4)

                logger.debug('Saved .MANIFEST => {0}'.format(manifest_file_path))
        except OSError:
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

    def _save_playlist_file(self, playlist_file_name, playlist_file_content):
        playlist_file_path = os.path.join(self._recording_directory_path, 'playlist', playlist_file_name)

        try:
            with open(playlist_file_path, 'w') as out_file:
                out_file.write(playlist_file_content)

                logger.debug('Saved playlist => {0}'.format(playlist_file_path))
        except OSError:
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

    def _save_segment_file(self, segment_file_name, segments_file_content):
        segment_file_path = os.path.join(self._recording_directory_path, 'segments', segment_file_name)

        try:
            with open(segment_file_path, 'wb') as out_file:
                out_file.write(segments_file_content)

                logger.debug('Saved segment => {0}'.format(segment_file_path))
        except OSError:
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

    def _set_stop_recording_event(self):
        self._stop_recording_event.set()

        logger.debug('Stopping recording of {0}'.format(self._recording.program_title))

    def run(self):
        logger.info('Starting recording of {0}'.format(self._recording.program_title))
        actual_start_date_time_in_utc = datetime.now(pytz.utc)

        self._create_recording_directory_tree()
        self._save_manifest_file(None,
                                 actual_start_date_time_in_utc.strftime('%Y-%m-%d %H:%M:%S%z'),
                                 None,
                                 'Started')

        for number_of_times_attempted_to_download_playlist_m3u8 in range(1, 11):
            # <editor-fold desc="Download playlist.m3u8">
            (response_status_code, response_text) = SmoothStreamsProxy.download_playlist_m3u8(
                '/playlist.m3u8?channel_number={0}&client_uuid={1}&protocol=hls'.format(self._recording.channel_number,
                                                                                        self._id),
                self._recording.channel_number,
                self._id,
                'hls',
                '0.0.0.0')
            # </editor-fold>
            if response_status_code == requests.codes.OK:
                self._save_manifest_file(None,
                                         actual_start_date_time_in_utc.strftime('%Y-%m-%d %H:%M:%S%z'),
                                         None,
                                         'In Progress')

                last_chunks_downloaded_m3u8_object = m3u8.loads(response_text)
                chunks_url = '/{0}'.format(last_chunks_downloaded_m3u8_object.data['playlists'][0]['uri'])

                break
            else:
                time_to_sleep_before_next_attempt = math.ceil(
                    number_of_times_attempted_to_download_playlist_m3u8 / 5) * 5

                logger.error('Attempt #{0} => Failed to download playlist.m3u8\nWill try again in {1} seconds'.format(
                    number_of_times_attempted_to_download_playlist_m3u8,
                    time_to_sleep_before_next_attempt))

                time.sleep(time_to_sleep_before_next_attempt)
        else:
            logger.error('Exhausted attempts to download playlist.m3u8')
            logger.info('Canceling recording of {0}'.format(self._recording.program_title))

            self._save_manifest_file(datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S%z'),
                                     actual_start_date_time_in_utc.strftime('%Y-%m-%d %H:%M:%S%z'),
                                     None,
                                     'Canceled')

            return

        vod_playlist_m3u8_object = None
        downloaded_segment_file_names = []

        while not self._stop_recording_event.is_set():
            # <editor-fold desc="Download chunks.m3u8">
            (channel_number, client_uuid, nimble_session_id_in_request, smooth_streams_hash_in_request) = \
                SmoothStreamsProxy.parse_query_string(chunks_url,
                                                      {'channel_number': None,
                                                       'client_uuid': None,
                                                       'nimblesessionid': None,
                                                       'wmsAuthSign': None})

            chunks_url = SmoothStreamsProxy.process_path_mapping(chunks_url,
                                                                 channel_number,
                                                                 client_uuid,
                                                                 nimble_session_id_in_request,
                                                                 smooth_streams_hash_in_request,
                                                                 '0.0.0.0')

            (response_status_code, response_text) = SmoothStreamsProxy.download_chunks_m3u8(chunks_url,
                                                                                            channel_number,
                                                                                            client_uuid,
                                                                                            '0.0.0.0')
            # </editor-fold>
            if response_status_code == requests.codes.OK:
                last_chunks_downloaded_date_time_in_utc = datetime.now(pytz.utc)
                last_chunks_downloaded_total_duration = 0
                last_chunks_downloaded_m3u8_object = m3u8.loads(response_text)

                if not vod_playlist_m3u8_object:
                    vod_playlist_m3u8_object = last_chunks_downloaded_m3u8_object

                indices_of_skipped_segments = []
                for (segment_index, segment) in enumerate(last_chunks_downloaded_m3u8_object.segments):
                    segment_uri = segment.uri
                    segment_file_name = urllib.parse.urlsplit(segment_uri).path
                    segment_path = '/{0}'.format(segment_uri)

                    last_chunks_downloaded_total_duration += segment.duration

                    if segment_file_name not in downloaded_segment_file_names:
                        # <editor-fold desc="Download ts file">
                        (channel_number, client_uuid) = \
                            SmoothStreamsProxy.parse_query_string(segment_path,
                                                                  {'channel_number': None, 'client_uuid': None})

                        (response_status_code, response_content) = SmoothStreamsProxy.download_ts_file(segment_path,
                                                                                                       channel_number,
                                                                                                       client_uuid,
                                                                                                       '0.0.0.0')
                        # </editor-fold>
                        if response_status_code == requests.codes.OK:
                            logger.debug('Downloaded segment\nSegment => {0}'.format(segment_file_name))

                            downloaded_segment_file_names.append(segment_file_name)
                            self._save_segment_file(segment_file_name, response_content)

                            segment.uri = '{0}?client_uuid={1}&program_title={2}'.format(
                                segment_file_name,
                                client_uuid,
                                urllib.parse.quote(self._recording.base_recording_directory))

                            if segment not in vod_playlist_m3u8_object.segments:
                                vod_playlist_m3u8_object.segments.append(segment)
                        else:
                            logger.error('Failed to download segment\nSegment => {0}'.format(segment_file_name))
                    else:
                        logger.debug('Skipped segment since it was already downloaded\nSegment => {0} '.format(
                            segment_file_name))

                        indices_of_skipped_segments.append(segment_index)

                for segment_index_to_delete in indices_of_skipped_segments:
                    del last_chunks_downloaded_m3u8_object.segments[segment_index_to_delete]
            else:
                logger.error('Failed to download chunks.m3u8')

                return

            current_date_time_in_utc = datetime.now(pytz.utc)
            wait_duration = last_chunks_downloaded_total_duration - (
                    current_date_time_in_utc - last_chunks_downloaded_date_time_in_utc).total_seconds()
            if wait_duration > 0:
                self._stop_recording_event.wait(wait_duration)

        if vod_playlist_m3u8_object:
            vod_playlist_m3u8_object.playlist_type = 'VOD'
            self._save_playlist_file('playlist.m3u8',
                                     '{0}\n{1}'.format(vod_playlist_m3u8_object.dumps(), '#EXT-X-ENDLIST'))

        self._save_manifest_file(datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S%z'),
                                 actual_start_date_time_in_utc.strftime('%Y-%m-%d %H:%M:%S%z'),
                                 'playlist.m3u8',
                                 'Completed')

        SmoothStreamsProxy.delete_active_recording(self._recording)

        logger.info('Finished recording of {0}'.format(self._recording.program_title))


class SmoothStreamsProxyHTTPRequestHandler(BaseHTTPRequestHandler):
    _lock = threading.RLock()

    def _send_http_response(self, client_ip_address, client_uuid, path, response_status_code, response_headers,
                            response_content, do_print_content=True):
        self.send_response(response_status_code)

        headers = []
        if response_headers:
            for header_entry in sorted(response_headers):
                self.send_header(header_entry, response_headers[header_entry])
                headers.append(
                    '{0:32} => {1!s}'.format(header_entry, response_headers[header_entry]))
        self.end_headers()

        # noinspection PyUnresolvedReferences
        logger.trace(
            'Response to {0}{1} for {2}:\n'
            '[Status Code]\n=============\n{3}\n\n'
            '{4}'
            '{5}'.format(client_ip_address,
                         '/{0}'.format(client_uuid) if client_uuid else '',
                         path,
                         response_status_code,
                         '[Header]\n========\n{0}\n\n'.format(
                             '\n'.join(headers)) if headers else '',
                         '[Content]\n=========\n{0:{1}}\n'.format(
                             response_content if do_print_content else len(response_content),
                             '' if do_print_content else ',') if response_content else ''))

        if response_content:
            try:
                self.wfile.write(bytes(response_content, 'utf-8'))
            except TypeError:
                self.wfile.write(response_content)

    # noinspection PyPep8Naming
    def do_DELETE(self):
        # noinspection PyBroadException
        try:
            client_address = self.client_address
            client_ip_address = client_address[0]
            path = self.path

            logger.debug('{0} requested from {1}\nRequest type => DELETE'.format(path, client_ip_address))

            if path.find('recordings') != -1:
                content_length = int(self.headers.get('Content-Length'))
                post_data = self.rfile.read(content_length)

                (channel_number, end_date_time_in_utc, program_title, start_date_time_in_utc) = \
                    SmoothStreamsProxy.parse_query_string('{0}?{1}'.format(path, post_data.decode()),
                                                          {'channel_number': None,
                                                           'end_date_time_in_utc': None,
                                                           'program_title': None,
                                                           'start_date_time_in_utc': None})

                end_date_time_in_utc = datetime.strptime(end_date_time_in_utc,
                                                         '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
                start_date_time_in_utc = datetime.strptime(start_date_time_in_utc,
                                                           '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)

                recording = Recording(channel_number, end_date_time_in_utc, program_title, start_date_time_in_utc)

                try:
                    SmoothStreamsProxy.delete_scheduled_recording(recording)

                    logger.info(
                        'Deleted scheduled recording:\n'
                        'Channel name      => {0}\n'
                        'Channel number    => {1}\n'
                        'Program title     => {2}\n'
                        'Start date & time => {3}\n'
                        'End date & time   => {4}'.format(SmoothStreamsProxy.get_channel_name(int(channel_number)),
                                                          channel_number,
                                                          program_title,
                                                          start_date_time_in_utc.astimezone(
                                                              get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                                                          end_date_time_in_utc.astimezone(
                                                              get_localzone()).strftime('%Y-%m-%d %H:%M:%S')))

                    self._send_http_response(client_ip_address,
                                             None,
                                             path,
                                             requests.codes.NO_CONTENT,
                                             None,
                                             None)
                except ValueError:
                    logger.error(
                        'Failed to find scheduled recording:\n'
                        'Channel name      => {0}\n'
                        'Channel number    => {1}\n'
                        'Program title     => {2}\n'
                        'Start date & time => {3}\n'
                        'End date & time   => {4}'.format(
                            SmoothStreamsProxy.get_channel_name(int(channel_number)),
                            channel_number,
                            program_title,
                            start_date_time_in_utc.astimezone(get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                            end_date_time_in_utc.astimezone(get_localzone()).strftime('%Y-%m-%d %H:%M:%S')))

                    self.send_error(requests.codes.NOT_FOUND)
            else:
                logger.error('HTTP error {0} encountered requesting {1} for {2}'.format(requests.codes.NOT_FOUND,
                                                                                        path,
                                                                                        client_ip_address))

                self.send_error(requests.codes.NOT_FOUND)
        except Exception:
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

            self.send_error(requests.codes.INTERNAL_SERVER_ERROR)

    # noinspection PyPep8Naming
    def do_GET(self):
        # noinspection PyBroadException
        try:
            client_address = self.client_address
            client_ip_address = client_address[0]
            path = self.path

            logger.debug('{0} requested from {1}\nRequest type => GET'.format(path, client_ip_address))

            if path.find('live') != -1:
                if path.find('.ts') != -1:
                    (channel_number, client_uuid) = \
                        SmoothStreamsProxy.parse_query_string(path, {'channel_number': None, 'client_uuid': None})

                    (response_status_code, response_content) = SmoothStreamsProxy.download_ts_file(path,
                                                                                                   channel_number,
                                                                                                   client_uuid,
                                                                                                   client_ip_address)
                    if response_status_code == requests.codes.OK:
                        self._send_http_response(client_ip_address,
                                                 client_uuid,
                                                 path,
                                                 response_status_code,
                                                 SmoothStreamsProxy.prepare_response_headers(response_content,
                                                                                             'video/m2ts'),
                                                 response_content,
                                                 False)
                    else:
                        self.send_error(response_status_code)
                elif path.find('chunks.m3u8') != -1:
                    (channel_number, client_uuid, nimble_session_id_in_request, smooth_streams_hash_in_request) = \
                        SmoothStreamsProxy.parse_query_string(path,
                                                              {'channel_number': None,
                                                               'client_uuid': None,
                                                               'nimblesessionid': None,
                                                               'wmsAuthSign': None})

                    path = SmoothStreamsProxy.process_path_mapping(path,
                                                                   channel_number,
                                                                   client_uuid,
                                                                   nimble_session_id_in_request,
                                                                   smooth_streams_hash_in_request,
                                                                   client_ip_address)

                    (response_status_code, response_text) = SmoothStreamsProxy.download_chunks_m3u8(path,
                                                                                                    channel_number,
                                                                                                    client_uuid,
                                                                                                    client_ip_address)
                    if response_status_code == requests.codes.OK:
                        self._send_http_response(client_ip_address,
                                                 client_uuid,
                                                 path,
                                                 response_status_code,
                                                 SmoothStreamsProxy.prepare_response_headers(
                                                     response_text,
                                                     'application/vnd.apple.mpegurl'),
                                                 response_text)
                    else:
                        self.send_error(response_status_code)
                elif path.find('epg.xml') != -1:
                    (number_of_days,) = SmoothStreamsProxy.parse_query_string(path, {'number_of_days': 1})
                    epg_file_name = 'xmltv{0}.xml.gz'.format(number_of_days)

                    (response_status_code, epg_xml_content) = SmoothStreamsProxy.get_file_content(epg_file_name)
                    if response_status_code == requests.codes.OK:
                        self._send_http_response(client_ip_address,
                                                 None,
                                                 path,
                                                 requests.codes.OK,
                                                 SmoothStreamsProxy.prepare_response_headers(epg_xml_content,
                                                                                             'application/xml'),
                                                 epg_xml_content,
                                                 do_print_content=False)
                    else:
                        self.send_error(response_status_code)
                elif path.find('playlist.m3u8') != -1:
                    protocol = SmoothStreamsProxy.get_configuration_parameter('SMOOTH_STREAMS_PROTOCOL')
                    do_generate_playlist_m3u8 = False

                    if '?' in path:
                        (channel_number, client_uuid, protocol) = SmoothStreamsProxy.parse_query_string(
                            path,
                            {'channel_number': None,
                             'client_uuid': None,
                             'protocol': protocol})

                        if channel_number:
                            logger.info('{0} requested from {1}/{2}'.format(
                                SmoothStreamsProxy.get_channel_name(int(channel_number)),
                                client_ip_address,
                                client_uuid))

                            (response_status_code, response_text) = SmoothStreamsProxy.download_playlist_m3u8(
                                path,
                                channel_number,
                                client_uuid,
                                protocol,
                                client_ip_address)
                            if response_status_code == requests.codes.OK:
                                self._send_http_response(client_ip_address,
                                                         client_uuid,
                                                         path,
                                                         response_status_code,
                                                         SmoothStreamsProxy.prepare_response_headers(
                                                             response_text,
                                                             'application/vnd.apple.mpegurl'),
                                                         response_text)
                            else:
                                self.send_error(response_status_code)
                        elif protocol:
                            do_generate_playlist_m3u8 = True
                        else:
                            logger.error('{0} requested from {1}/{2} has an invalid query string'.format(
                                SmoothStreamsProxy.get_channel_name(int(channel_number)),
                                client_ip_address,
                                client_uuid))

                            self.send_error(requests.codes.BAD_REQUEST)
                    else:
                        do_generate_playlist_m3u8 = True

                    if do_generate_playlist_m3u8:
                        (response_status_code, channels_json_content) = SmoothStreamsProxy.get_file_content(
                            'channels.json')
                        if response_status_code == requests.codes.OK:
                            (response_status_code, playlist_m3u8_content) = \
                                SmoothStreamsProxy.generate_live_playlist_m3u8(json.loads(channels_json_content),
                                                                               protocol)
                            if response_status_code == requests.codes.OK:
                                self._send_http_response(client_ip_address,
                                                         None,
                                                         path,
                                                         requests.codes.OK,
                                                         SmoothStreamsProxy.prepare_response_headers(
                                                             playlist_m3u8_content,
                                                             'application/vnd.apple.mpegurl'),
                                                         playlist_m3u8_content)
                            else:
                                self.send_error(response_status_code)
                        else:
                            self.send_error(response_status_code)
                else:
                    logger.error('HTTP error {0} encountered requesting {1} for {2}'.format(requests.codes.NOT_FOUND,
                                                                                            path,
                                                                                            client_ip_address))

                    self.send_error(requests.codes.NOT_FOUND)
            elif path.find('recordings') != -1:
                (recording_type,) = SmoothStreamsProxy.parse_query_string(path, {'type': None})
                if recording_type:
                    recording_type = recording_type.lower()

                    if recording_type not in ['active', 'persisted', 'scheduled']:
                        logger.error('type: {0} sent in the query string is not supported'.format(recording_type))

                        self.send_error(requests.codes.BAD_REQUEST)

                        return

                recordings = {}

                if recording_type == 'active' or not recording_type:
                    for active_recording in SmoothStreamsProxy.get_active_recordings():
                        recordings.setdefault('active_recordings', []).append(
                            {'channel_name': SmoothStreamsProxy.get_channel_name(int(active_recording.channel_number)),
                             'channel_number': active_recording.channel_number,
                             'end_date_time_in_utc': '{0}'.format(active_recording.end_date_time_in_utc),
                             'program_title': active_recording.program_title,
                             'start_date_time_in_utc': '{0}'.format(active_recording.start_date_time_in_utc)})

                if recording_type == 'persisted' or not recording_type:
                    for persisted_recording in SmoothStreamsProxy.get_persistent_recordings():
                        recordings.setdefault('persisted_recordings', []).append(
                            {'channel_name': SmoothStreamsProxy.get_channel_name(
                                int(persisted_recording.channel_number)),
                                'channel_number': persisted_recording.channel_number,
                                'end_date_time_in_utc': '{0}'.format(persisted_recording.end_date_time_in_utc),
                                'program_title': persisted_recording.program_title,
                                'start_date_time_in_utc': '{0}'.format(persisted_recording.start_date_time_in_utc)})

                if recording_type == 'scheduled' or not recording_type:
                    for scheduled_recording in SmoothStreamsProxy.get_scheduled_recordings():
                        recordings.setdefault('scheduled_recordings', []).append(
                            {'channel_name': SmoothStreamsProxy.get_channel_name(
                                int(scheduled_recording.channel_number)),
                                'channel_number': scheduled_recording.channel_number,
                                'end_date_time_in_utc': '{0}'.format(scheduled_recording.end_date_time_in_utc),
                                'program_title': scheduled_recording.program_title,
                                'start_date_time_in_utc': '{0}'.format(scheduled_recording.start_date_time_in_utc)})

                response_json = json.dumps(recordings, sort_keys=True, indent=4)

                self._send_http_response(client_ip_address,
                                         None,
                                         path,
                                         requests.codes.OK,
                                         SmoothStreamsProxy.prepare_response_headers(response_json, 'application/json'),
                                         response_json)
            elif path.find('vod') != -1:
                if path.find('.ts') != -1:
                    (client_uuid, program_title) = SmoothStreamsProxy.parse_query_string(path,
                                                                                         {'client_uuid': None,
                                                                                          'program_title': None})

                    ts_file_content = SmoothStreamsProxy.read_ts_file(path, program_title)
                    if ts_file_content:
                        self._send_http_response(client_ip_address,
                                                 client_uuid,
                                                 path,
                                                 requests.codes.OK,
                                                 SmoothStreamsProxy.prepare_response_headers(ts_file_content,
                                                                                             'video/m2ts'),
                                                 ts_file_content,
                                                 False)
                    else:
                        self.send_error(requests.codes.NOT_FOUND)
                elif path.find('playlist.m3u8'):
                    if '?' in path:
                        (client_uuid, program_title) = SmoothStreamsProxy.parse_query_string(path,
                                                                                             {'client_uuid': None,
                                                                                              'program_title': None})

                        logger.info('{0} requested from {1}/{2}'.format(
                            base64.urlsafe_b64decode(program_title.encode()).decode(),
                            client_ip_address,
                            client_uuid))

                        vod_playlist_m3u8_content = SmoothStreamsProxy.read_vod_playlist_m3u8(program_title)
                        if vod_playlist_m3u8_content:
                            self._send_http_response(client_ip_address,
                                                     None,
                                                     path,
                                                     requests.codes.OK,
                                                     SmoothStreamsProxy.prepare_response_headers(
                                                         vod_playlist_m3u8_content,
                                                         'application/vnd.apple.mpegurl'),
                                                     vod_playlist_m3u8_content)
                        else:
                            self.send_error(requests.codes.NOT_FOUND)
                    else:
                        playlist_m3u8_content = SmoothStreamsProxy.generate_vod_playlist_m3u8()
                        if playlist_m3u8_content:
                            self._send_http_response(client_ip_address,
                                                     None,
                                                     path,
                                                     requests.codes.OK,
                                                     SmoothStreamsProxy.prepare_response_headers(
                                                         playlist_m3u8_content,
                                                         'application/vnd.apple.mpegurl'),
                                                     playlist_m3u8_content)
                        else:
                            self.send_error(requests.codes.NOT_FOUND)
            else:
                logger.error('HTTP error {0} encountered requesting {1} for {2}'.format(requests.codes.NOT_FOUND,
                                                                                        path,
                                                                                        client_ip_address))

                self.send_error(requests.codes.NOT_FOUND)
        except Exception:
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

            self.send_error(requests.codes.INTERNAL_SERVER_ERROR)

    # noinspection PyPep8Naming
    def do_POST(self):
        # noinspection PyBroadException
        try:
            client_address = self.client_address
            client_ip_address = client_address[0]
            path = self.path

            logger.debug('{0} requested from {1}\nRequest type => POST'.format(path, client_ip_address))

            if path.find('recordings') != -1:
                content_length = int(self.headers.get('Content-Length'))
                post_data = self.rfile.read(content_length)

                (channel_number, end_date_time_in_utc, program_title, start_date_time_in_utc) = \
                    SmoothStreamsProxy.parse_query_string('{0}?{1}'.format(path, post_data.decode()),
                                                          {'channel_number': None,
                                                           'end_date_time_in_utc': None,
                                                           'program_title': None,
                                                           'start_date_time_in_utc': None})

                end_date_time_in_utc = datetime.strptime(end_date_time_in_utc,
                                                         '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
                start_date_time_in_utc = datetime.strptime(start_date_time_in_utc,
                                                           '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)

                recording = Recording(channel_number, end_date_time_in_utc, program_title, start_date_time_in_utc)

                try:
                    SmoothStreamsProxy.add_scheduled_recording(recording)

                    logger.info(
                        'Scheduling recording:\n'
                        'Channel name      => {0}\n'
                        'Channel number    => {1}\n'
                        'Program title     => {2}\n'
                        'Start date & time => {3}\n'
                        'End date & time   => {4}'.format(SmoothStreamsProxy.get_channel_name(int(channel_number)),
                                                          channel_number,
                                                          program_title,
                                                          start_date_time_in_utc.astimezone(
                                                              get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                                                          end_date_time_in_utc.astimezone(
                                                              get_localzone()).strftime('%Y-%m-%d %H:%M:%S')))

                    response_json = json.dumps(
                        {'channel_name': SmoothStreamsProxy.get_channel_name(int(channel_number)),
                         'channel_number': channel_number,
                         'end_date_time_in_utc': '{0}'.format(end_date_time_in_utc),
                         'program_title': program_title,
                         'start_date_time_in_utc': '{0}'.format(start_date_time_in_utc)},
                        sort_keys=True,
                        indent=4)

                    self._send_http_response(client_ip_address,
                                             None,
                                             path,
                                             requests.codes.CREATED,
                                             SmoothStreamsProxy.prepare_response_headers(response_json,
                                                                                         'application/json'),
                                             response_json)
                except DuplicateRecordingError:
                    logger.info(
                        'Recording already scheduled:\n'
                        'Channel name      => {0}\n'
                        'Channel number    => {1}\n'
                        'Program title     => {2}\n'
                        'Start date & time => {3}\n'
                        'End date & time   => {4}'.format(SmoothStreamsProxy.get_channel_name(int(channel_number)),
                                                          channel_number,
                                                          program_title,
                                                          start_date_time_in_utc.astimezone(
                                                              get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                                                          end_date_time_in_utc.astimezone(
                                                              get_localzone()).strftime('%Y-%m-%d %H:%M:%S')))

                    self.send_error(requests.codes.CONFLICT)
            else:
                logger.error('HTTP error {0} encountered requesting {1} for {2}'.format(requests.codes.NOT_FOUND,
                                                                                        path,
                                                                                        client_ip_address))

                self.send_error(requests.codes.NOT_FOUND)
        except Exception:
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

            self.send_error(requests.codes.INTERNAL_SERVER_ERROR)

    def log_message(self, format_, *args):
        return


class SmoothStreamsProxyHTTPRequestHandlerThread(threading.Thread):
    def __init__(self, server_address, server_socket):
        threading.Thread.__init__(self)

        self.server_address = server_address
        self.server_socket = server_socket
        self.server_close = lambda self_: None

        self.daemon = True
        self.start()

    def run(self):
        smooth_streams_proxy_http_server = HTTPServer(self.server_address, SmoothStreamsProxyHTTPRequestHandler, False)

        smooth_streams_proxy_http_server.socket = self.server_socket
        smooth_streams_proxy_http_server.server_bind = self.server_close

        smooth_streams_proxy_http_server.serve_forever()


class SmoothStreamsProxyConfigurationEventHandler(FileSystemEventHandler):
    _last_modification_date_time = None
    _lock = threading.RLock()

    def on_modified(self, event):
        with SmoothStreamsProxyConfigurationEventHandler._lock:
            configuration_file = SmoothStreamsProxy.get_configuration_file_path()
            if event.src_path == configuration_file:
                read_configuration_file = False
                last_modification_date_time = SmoothStreamsProxyConfigurationEventHandler._last_modification_date_time
                current_date_time_in_utc = datetime.now(pytz.utc)

                # Read the configuration file if this is the first modification since the proxy started or if the
                # modification events are at least 1s apart (A hack to deal with watchdog generating duplicate events)
                if not last_modification_date_time:
                    read_configuration_file = True

                    SmoothStreamsProxyConfigurationEventHandler._last_modification_date_time = current_date_time_in_utc
                else:
                    total_time_between_modifications = (
                            current_date_time_in_utc - last_modification_date_time).total_seconds()

                    if total_time_between_modifications >= 1.0:
                        read_configuration_file = True

                        SmoothStreamsProxyConfigurationEventHandler._last_modification_date_time = \
                            current_date_time_in_utc

                if read_configuration_file:
                    SmoothStreamsProxy.read_configuration_file(initial_read=False)

                    try:
                        SmoothStreamsProxy.set_logging_level(getattr(logging,
                                                                     SmoothStreamsProxy.get_configuration_parameter(
                                                                         'LOGGING_LEVEL').upper()))
                    except AttributeError:
                        (type_, value_, traceback_) = sys.exc_info()
                        logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))


class SmoothStreamsProxy():
    _active_recordings = []
    _active_recordings_lock = threading.RLock()
    _channel_map = {}
    _channel_map_lock = threading.RLock()
    _configuration = {}
    _configuration_file_path = None
    _configuration_lock = threading.RLock()
    _epg_source_urls = ['https://sstv.fog.pt/epg', 'http://ca.epgrepo.download', 'http://eu.epgrepo.download']
    _fernet_key = None
    _files_map = {}
    _files_map_lock = threading.RLock()
    _http_request_handler_threads = None
    _log_file_path = None
    _nimble_session_id_map = {}
    _nimble_session_id_map_lock = threading.RLock()
    _recordings_directory_path = None
    _refresh_session_timer = None
    _scheduled_recordings = []
    _scheduled_recordings_lock = threading.RLock()
    _serviceable_clients = {}
    _serviceable_clients_lock = threading.RLock()
    _session = {}
    _session_lock = threading.RLock()
    _start_recording_timer = None

    @classmethod
    def _cleanup_shelf(cls):
        shelf_file_path = os.path.join(os.getcwd(), 'smooth_streams_proxy_db')

        if os.path.exists(shelf_file_path):
            logger.debug('Cleaning up shelved settings from {0}'.format(shelf_file_path))

            with shelve.open(shelf_file_path) as smooth_streams_proxy_db:
                try:
                    session = smooth_streams_proxy_db['session']
                    hash_expires_on = session['expires_on']
                    current_date_time_in_utc = datetime.now(pytz.utc)

                    if current_date_time_in_utc >= hash_expires_on:
                        logger.debug('Deleting expired shelved session:\nHash => {0}\nExpired On => {1}'.format(
                            session['hash'],
                            session['expires_on'].astimezone(get_localzone()).strftime('%Y-%m-%d %H:%M:%S')[:-3]))
                        del smooth_streams_proxy_db['session']
                except KeyError:
                    (type_, value_, traceback_) = sys.exc_info()
                    logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

                try:
                    files_map = smooth_streams_proxy_db['files_map']
                    for file_name in sorted(files_map):
                        next_update_date_time = files_map[file_name]['next_update_date_time']
                        current_date_time_in_utc = datetime.now(pytz.utc)

                        if current_date_time_in_utc >= next_update_date_time:
                            logger.debug('Deleting expired shelved file:\n{0}'.format(
                                'File name  => {0}\n'
                                'File size  => {1:,}\n'
                                'Expired on => {2}'.format(file_name,
                                                           files_map[file_name]['size'],
                                                           next_update_date_time.astimezone(
                                                               get_localzone()).strftime('%Y-%m-%d %H:%M:%S'))))

                            try:
                                os.remove(files_map[file_name][file_name]['source'])
                            except OSError:
                                (type_, value_, traceback_) = sys.exc_info()
                                logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

                            del files_map[file_name]
                    smooth_streams_proxy_db['files_map'] = files_map
                except KeyError:
                    (type_, value_, traceback_) = sys.exc_info()
                    logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

                try:
                    scheduled_recordings = smooth_streams_proxy_db['scheduled_recordings']
                    for scheduled_recording in scheduled_recordings:
                        scheduled_recording_end_date_time_in_utc = scheduled_recording.end_date_time_in_utc
                        current_date_time_in_utc = datetime.now(pytz.utc)

                        if current_date_time_in_utc >= scheduled_recording_end_date_time_in_utc:
                            logger.debug(
                                'Deleting expired scheduled recording:\n{0}'.format(
                                    'Channel name      => {0}\n'
                                    'Channel number    => {1}\n'
                                    'Program title     => {2}\n'
                                    'Start date & time => {3}\n'
                                    'End date & time   => {4}'.format(
                                        SmoothStreamsProxy.get_channel_name(int(scheduled_recording.channel_number)),
                                        scheduled_recording.channel_number,
                                        scheduled_recording.program_title,
                                        scheduled_recording.start_date_time_in_utc.astimezone(get_localzone()).strftime(
                                            '%Y-%m-%d %H:%M:%S'),
                                        scheduled_recording.end_date_time_in_utc.astimezone(get_localzone()).strftime(
                                            '%Y-%m-%d %H:%M:%S'))))

                            del scheduled_recordings[scheduled_recording]
                    smooth_streams_proxy_db['scheduled_recordings'] = scheduled_recordings
                except KeyError:
                    (type_, value_, traceback_) = sys.exc_info()
                    logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

                try:
                    active_recordings = smooth_streams_proxy_db['active_recordings']
                    for active_recording in active_recordings:
                        active_recording_end_date_time_in_utc = active_recording.end_date_time_in_utc
                        current_date_time_in_utc = datetime.now(pytz.utc)

                        if current_date_time_in_utc >= active_recording_end_date_time_in_utc:
                            logger.debug(
                                'Deleting expired active recording:\n{0}'.format(
                                    'Channel name      => {0}\n'
                                    'Channel number    => {1}\n'
                                    'Program title     => {2}\n'
                                    'Start date & time => {3}\n'
                                    'End date & time   => {4}'.format(
                                        SmoothStreamsProxy.get_channel_name(int(active_recording.channel_number)),
                                        active_recording.channel_number,
                                        active_recording.program_title,
                                        active_recording.start_date_time_in_utc.astimezone(get_localzone()).strftime(
                                            '%Y-%m-%d %H:%M:%S'),
                                        active_recording.end_date_time_in_utc.astimezone(get_localzone()).strftime(
                                            '%Y-%m-%d %H:%M:%S'))))

                            del active_recordings[active_recording]
                    smooth_streams_proxy_db['active_recordings'] = active_recordings
                except KeyError:
                    (type_, value_, traceback_) = sys.exc_info()
                    logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))
        else:
            logger.debug('Shelf file {0} does not exists. Nothing to cleanup'.format(shelf_file_path))

    @classmethod
    def _clear_nimble_session_id_map(cls):
        with cls._nimble_session_id_map_lock:
            cls._nimble_session_id_map = {}

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
            current_date_time_in_utc = datetime.now(pytz.utc)

            if current_date_time_in_utc < (hash_expires_on - timedelta(seconds=60)):
                return False
            else:
                logger.info('Authorization hash expired. Need to retrieve a new authorization hash')
                return True
        except KeyError:
            logger.debug('Authorization hash was never retrieved. Need to retrieve one')
            return True

    @classmethod
    def _download_file(cls, file_name):
        url = '{0}/{1}'.format(cls._get_epg_source_url(), file_name)

        logger.info('Downloading {0}'.format(url))

        session = requests.Session()
        response = cls._make_http_request(session.get, url, headers=session.headers)

        response_status_code = response.status_code
        if response_status_code == requests.codes.OK:
            response_headers = response.headers
            response_content = response.content if file_name.endswith('.gz') else json.dumps(response.json(),
                                                                                             sort_keys=True,
                                                                                             indent=2)

            # noinspection PyUnresolvedReferences
            logger.trace(
                'Response from {0}:\n'
                '[Status Code]\n=============\n{1}\n\n'
                '[Header]\n========\n{2}\n\n'
                '[Content]\n=========\n{3:,}\n'.format(url, response_status_code, '\n'.join(
                    ['{0:32} => {1!s}'.format(key, response_headers[key])
                     for key in sorted(response_headers)]), len(response_content)))

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

                logger.debug('{0} written to {1}'.format(file_name, file_path))

            if file_name in cls._files_map:
                try:
                    os.remove(cls._files_map[file_name]['source'])
                except OSError:
                    (type_, value_, traceback_) = sys.exc_info()
                    logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

            cls._files_map[file_name] = {}
            cls._files_map[file_name]['size'] = len(response_content)
            cls._files_map[file_name]['source'] = file_path
            cls._files_map[file_name]['next_update_date_time'] = current_date_time_in_utc.replace(
                microsecond=0,
                second=0,
                minute=0,
                hour=0) + timedelta(hours=(((current_date_time_in_utc.hour // 4) * 4) + 3) +
                                          ((current_date_time_in_utc.minute // 33) *
                                           (4 if (current_date_time_in_utc.hour + 1) % 4 == 0 else 0)),
                                    minutes=32)

            cls._persist_to_shelf('files_map', cls._files_map)

            return (response_status_code, response_content)
        else:
            return (response_status_code, None)

    @classmethod
    def _encrypt_password(cls):
        cls._fernet_key = Fernet.generate_key()
        fernet = Fernet(cls._fernet_key)
        encrypted_smooth_streams_password = fernet.encrypt(
            cls._configuration['SMOOTH_STREAMS_PASSWORD'].encode()).decode()

        logger.debug('Encrypted SmoothStreams password to {0}'.format(encrypted_smooth_streams_password))

        cls._configuration['SMOOTH_STREAMS_PASSWORD'] = encrypted_smooth_streams_password

    @classmethod
    def _get_epg_source_url(cls):
        return cls._epg_source_urls[random.randint(0, len(cls._epg_source_urls) - 1)]

    @classmethod
    def _get_mapped_nimble_session_id(cls, hijacked_nimble_session_id):
        with cls._nimble_session_id_map_lock:
            return cls._nimble_session_id_map.get(hijacked_nimble_session_id, None)

    @classmethod
    def _get_session_parameter(cls, parameter_name):
        with cls._session_lock:
            return cls._session[parameter_name]

    @classmethod
    def _initialize_logging(cls):
        logging.addLevelName(TRACE, 'TRACE')
        logging.TRACE = TRACE
        logging.trace = trace
        logging.Logger.trace = trace

        formatter = MultiLineFormatter('%(asctime)s %(module)-20s %(funcName)-40s %(levelname)-8s %(message)s')

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        rotating_file_handler = logging.handlers.RotatingFileHandler('{0}'.format(cls._log_file_path),
                                                                     maxBytes=1024 * 1024 * 10,
                                                                     backupCount=10)
        rotating_file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(rotating_file_handler)

        cls.set_logging_level(logging.INFO)

    @classmethod
    def _hijack_nimble_session_id(cls, hijacked_nimble_session_id, hijacking_nimble_session_id):
        with cls._nimble_session_id_map_lock:
            cls._nimble_session_id_map[hijacked_nimble_session_id] = hijacking_nimble_session_id

    @classmethod
    def _load_shelved_settings(cls):
        file_path = os.path.join(os.getcwd(), 'smooth_streams_proxy_db')

        logger.debug('Attempting to load shelved settings from {0}'.format(file_path))

        with shelve.open(file_path) as smooth_streams_proxy_db:
            try:
                cls._session = smooth_streams_proxy_db['session']

                logger.debug('Loaded shelved session:\nHash     => {0}\nValid to => {1}'.format(
                    cls._session['hash'],
                    cls._session['expires_on'].astimezone(get_localzone()).strftime('%Y-%m-%d %H:%M:%S')))
            except KeyError:
                logger.debug('Failed to load shelved session from {0}'.format(os.path.join(os.getcwd(),
                                                                                           'smooth_streams_proxy_db')))

            try:
                cls._files_map = smooth_streams_proxy_db['files_map']

                if cls._files_map:
                    logger.debug('Loaded shelved files:\n{0}'.format(
                        '\n'.join(
                            ['File name => {0}\nFile size => {1:,}\nValid to  => {2}\n'.format(
                                file_name,
                                cls._files_map[file_name]['size'],
                                cls._files_map[file_name]['next_update_date_time'].astimezone(
                                    get_localzone()).strftime('%Y-%m-%d %H:%M:%S')) for file_name
                                in sorted(cls._files_map)]).strip()))
            except KeyError:
                logger.debug(
                    'Failed to load files from {0}'.format(os.path.join(os.getcwd(), 'smooth_streams_proxy_db')))

            try:
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

            try:
                cls._scheduled_recordings = smooth_streams_proxy_db['scheduled_recordings']

                if cls._scheduled_recordings:
                    logger.debug(
                        'Loaded shelved scheduled recordings:\n{0}'.format(
                            '\n'.join([
                                'Channel name      => {0}\n'
                                'Channel number    => {1}\n'
                                'Program title     => {2}\n'
                                'Start date & time => {3}\n'
                                'End date & time   => {4}'.format(
                                    SmoothStreamsProxy.get_channel_name(int(recording.channel_number)),
                                    recording.channel_number,
                                    recording.program_title,
                                    recording.start_date_time_in_utc.astimezone(get_localzone()).strftime(
                                        '%Y-%m-%d %H:%M:%S'),
                                    recording.end_date_time_in_utc.astimezone(get_localzone()).strftime(
                                        '%Y-%m-%d %H:%M:%S')) for recording in cls._scheduled_recordings]).strip()))
            except KeyError:
                logger.debug('Failed to load scheduled recordings from {0}'.format(os.path.join(
                    os.getcwd(),
                    'smooth_streams_proxy_db')))

            try:
                cls._active_recordings = smooth_streams_proxy_db['active_recordings']

                if cls._active_recordings:
                    logger.debug('Loaded shelved active recordings:\n{0}'.format('\n'.join(
                        ['Channel name      => {0}\n'
                         'Channel number    => {1}\n'
                         'Program title     => {2}\n'
                         'Start date & time => {3}\n'
                         'End date & time   => {4}\n'.format(
                            SmoothStreamsProxy.get_channel_name(int(recording.channel_number)),
                            recording.channel_number,
                            recording.program_title,
                            recording.start_date_time_in_utc.astimezone(get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                            recording.end_date_time_in_utc.astimezone(get_localzone()).strftime('%Y-%m-%d %H:%M:%S'))
                            for
                            recording in cls._active_recordings]).strip()))
            except KeyError:
                logger.debug('Failed to load active recordings from {0}'.format(os.path.join(
                    os.getcwd(),
                    'smooth_streams_proxy_db')))

    @classmethod
    def _make_http_request(cls, requests_http_method, url, params=None, data=None, json_=None, headers=None,
                           cookies=None, timeout=60):
        try:
            # noinspection PyUnresolvedReferences
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
        except requests.exceptions.RequestException as e:
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

            raise e

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
                    'Exiting...'.format(cls._configuration_file_path))

                sys.exit()

    @classmethod
    def _parse_command_line_arguments(cls):
        parser = argparse.ArgumentParser()

        parser.add_argument('-c',
                            action='store',
                            default='smooth_streams_proxy.ini',
                            dest='configuration_file_path',
                            help='path to the configuration file',
                            metavar='configuration file path')
        parser.add_argument('-l',
                            action='store',
                            default='logs/smooth_streams_proxy.log',
                            dest='log_file_path',
                            help='path to the log file',
                            metavar='log file path')
        parser.add_argument('-r',
                            action='store',
                            default='recordings',
                            dest='recordings_path',
                            help='path to the recordings folder',
                            metavar='recordings folder path')

        arguments = parser.parse_args()

        return (arguments.configuration_file_path, arguments.log_file_path, arguments.recordings_path)

    @classmethod
    def _persist_to_shelf(cls, key, value):
        file_path = os.path.join(os.getcwd(), 'smooth_streams_proxy_db')

        logger.debug('Attempting to persist {0} to {1}'.format(key, file_path))

        try:
            with shelve.open(file_path) as smooth_streams_proxy_db:
                smooth_streams_proxy_db[key] = value

                logger.debug('Persisted {0} to {1}'.format(key, file_path))
        except OSError:
            logger.debug('Failed to persist {0} to {1}'.format(key, file_path))

    @classmethod
    def _process_authorization_hash(cls, hash_response):
        if 'code' in hash_response:
            if hash_response['code'] == '0':
                logger.error('Failed to retrieved authorization token:\nError => {0}'.format(hash_response['error']))
            elif hash_response['code'] == '1':
                cls._session['hash'] = hash_response['hash']
                cls._session['expires_on'] = datetime.now(pytz.utc) + timedelta(seconds=(hash_response['valid'] * 60))

                logger.info('Retrieved authorization token:\nHash => {0}\nExpires On => {1}'.format(
                    cls._session['hash'],
                    cls._session['expires_on'].astimezone(get_localzone()).strftime('%Y-%m-%d %H:%M:%S')[:-3]))

                cls._persist_to_shelf('session', cls._session)
        else:
            logger.error('Failed to retrieved authorization token:\ncode not in response')

    @classmethod
    def _refresh_serviceable_clients(cls, client_uuid, client_ip_address):
        with cls._serviceable_clients_lock:
            if client_uuid not in cls._serviceable_clients:
                logger.debug('Adding {0}/{1} to serviceable clients'.format(client_ip_address, client_uuid))

                cls._serviceable_clients[client_uuid] = {}
                cls._serviceable_clients[client_uuid]['ip_address'] = client_ip_address
            else:
                cls._serviceable_clients[client_uuid]['last_request_date_time'] = datetime.now(pytz.utc)

    @classmethod
    def _refresh_session(cls):
        with cls._session_lock:
            response_status_code = requests.codes.OK
            do_start_timer = False

            if cls._do_retrieve_authorization_hash():
                do_start_timer = True

                cls._clear_nimble_session_id_map()

                response_status_code = cls._retrieve_authorization_hash()
                if response_status_code != requests.codes.OK:
                    do_start_timer = False

                if cls._refresh_session_timer:
                    cls._refresh_session_timer.cancel()
            elif not cls._refresh_session_timer:
                do_start_timer = True

            if do_start_timer:
                interval = (cls._session['expires_on'] - datetime.now(pytz.utc)).total_seconds() - 45
                cls._refresh_session_timer = threading.Timer(interval, cls._timed_refresh_session)
                cls._refresh_session_timer.start()

                logger.debug('Starting authorization hash refresh timer\nInterval => {0} seconds'.format(interval))

            return response_status_code

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
    def _restart_active_recording(cls):
        with cls._active_recordings_lock:
            for active_recording in cls._active_recordings:
                SmoothStreamsProxyRecordingThread(active_recording)

    @classmethod
    def _retrieve_authorization_hash(cls):
        logger.debug('Retrieving authorization hash')

        cls._session['http_session'] = None
        session = requests.Session()

        if cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE') == 'viewmmasr':
            url = 'https://www.mma-tv.net/loginForm.php'
        else:
            url = 'https://auth.smoothstreams.tv/hash_api.php'

        response = cls._make_http_request(
            session.get,
            url,
            params={
                'username': cls.get_configuration_parameter('SMOOTH_STREAMS_USERNAME'),
                'password': cls._decrypt_password().decode(),
                'site': cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE')},
            headers=session.headers,
            cookies=session.cookies.get_dict())

        response_status_code = response.status_code
        if response_status_code == requests.codes.OK:
            cls._session['http_session'] = session
        elif response_status_code != requests.codes.NOT_FOUND:
            logger.error('HTTP error {0} encountered requesting {1}'.format(response_status_code, url))

            return response_status_code

        response_headers = response.headers
        response_json = response.json()

        # noinspection PyUnresolvedReferences
        logger.trace(
            'Response from {0}:\n'
            '[Status Code]\n=============\n{1}\n\n'
            '[Header]\n========\n{2}\n\n'
            '[Content]\n=========\n{3}\n'.format(url, response_status_code,
                                                 '\n'.join(
                                                     ['{0:32} => {1!s}'.format(key, response_headers[key])
                                                      for key in sorted(response_headers)]),
                                                 json.dumps(response_json, sort_keys=True, indent=2)))

        cls._process_authorization_hash(response_json)

        return response_status_code

    @classmethod
    def _scrub_configuration_file(cls):
        cls._encrypt_password()
        cls._update_configuration_file('SmoothStreams',
                                       'password',
                                       cls._configuration['SMOOTH_STREAMS_PASSWORD'])
        cls._persist_to_shelf('fernet_key', cls._fernet_key)

    @classmethod
    def _set_start_recording_timer(cls):
        soonest_scheduled_recording_start_date_time_in_utc = None
        current_date_time_in_utc = datetime.now(pytz.utc)

        for scheduled_recording in cls._scheduled_recordings:
            scheduled_recording_start_date_time_in_utc = scheduled_recording.start_date_time_in_utc

            if current_date_time_in_utc > scheduled_recording_start_date_time_in_utc:
                if current_date_time_in_utc > scheduled_recording_start_date_time_in_utc:
                    cls.delete_scheduled_recording(scheduled_recording)
                    cls.add_active_recording(scheduled_recording)

                    SmoothStreamsProxyRecordingThread(scheduled_recording)
            elif not soonest_scheduled_recording_start_date_time_in_utc:
                soonest_scheduled_recording_start_date_time_in_utc = scheduled_recording_start_date_time_in_utc
            elif soonest_scheduled_recording_start_date_time_in_utc > scheduled_recording_start_date_time_in_utc:
                soonest_scheduled_recording_start_date_time_in_utc = scheduled_recording_start_date_time_in_utc

        if cls._start_recording_timer:
            cls._start_recording_timer.cancel()

        if soonest_scheduled_recording_start_date_time_in_utc:
            interval = (soonest_scheduled_recording_start_date_time_in_utc - datetime.now(pytz.utc)).total_seconds()
            cls._start_recording_timer = threading.Timer(interval, cls._start_recording)
            cls._start_recording_timer.start()

            logger.debug('Starting recording timer\nInterval => {0} seconds'.format(interval))

    @classmethod
    def _start_recording(cls):
        with cls._scheduled_recordings_lock:
            current_date_time_in_utc = datetime.now(pytz.utc)

            for scheduled_recording in cls._scheduled_recordings:
                scheduled_recording_start_date_time_in_utc = scheduled_recording.start_date_time_in_utc

                if current_date_time_in_utc > scheduled_recording_start_date_time_in_utc:
                    cls.delete_scheduled_recording(scheduled_recording)
                    cls.add_active_recording(scheduled_recording)

                    SmoothStreamsProxyRecordingThread(scheduled_recording)

            cls._set_start_recording_timer()

    @classmethod
    def _set_channel_map(cls, channel_map):
        with cls._channel_map_lock:
            cls._channel_map = channel_map

            cls._persist_to_shelf('channel_map', cls._channel_map)

    @classmethod
    def _timed_refresh_session(cls):
        logger.debug('Authorization hash refresh timer triggered')

        cls._refresh_session()

    @classmethod
    def _update_configuration_file(cls, section, option, value):
        with cls._configuration_lock:
            configuration = configparser.RawConfigParser()
            configuration.read(cls._configuration_file_path)

            configuration.set(section, option, value)

            with open(cls._configuration_file_path, 'w') as configuration_file:
                configuration.write(configuration_file)

                logger.debug('Updated configuration file {0}\nSection => {1}\nOption  => {2}\nValue  => {3}'.format(
                    cls._configuration_file_path, section, option, value))

    @classmethod
    def _validate_fernet_key(cls):
        try:
            cls._decrypt_password()

            logger.debug('Decryption key loaded is valid for the encrypted SmoothStreams password')
        except InvalidToken:
            logger.debug(
                'Decryption key loaded is not valid for the encrypted SmoothStreams password\n'
                'Please re-enter your cleartext password in the configuration file {0}\n'
                'Exiting...'.format(cls._configuration_file_path))

            cls._remove_from_shelf('fernet_key')

            sys.exit()

    @classmethod
    def add_active_recording(cls, recording):
        with cls._active_recordings_lock:
            cls._active_recordings.append(recording)

            cls._persist_to_shelf('active_recordings', cls._active_recordings)

    @classmethod
    def add_scheduled_recording(cls, recording):
        with cls._scheduled_recordings_lock:
            if recording not in cls._scheduled_recordings:
                cls._scheduled_recordings.append(recording)

                cls._persist_to_shelf('scheduled_recordings', cls._scheduled_recordings)

                cls._set_start_recording_timer()
            else:
                raise DuplicateRecordingError

    @classmethod
    def delete_active_recording(cls, recording):
        with cls._active_recordings_lock:
            cls._active_recordings.remove(recording)

            cls._persist_to_shelf('active_recordings', cls._active_recordings)

    @classmethod
    def delete_scheduled_recording(cls, recording):
        with cls._scheduled_recordings_lock:
            cls._scheduled_recordings.remove(recording)

            cls._persist_to_shelf('scheduled_recordings', cls._scheduled_recordings)

            cls._set_start_recording_timer()

    @classmethod
    def download_chunks_m3u8(cls, path, channel_number, client_uuid, client_ip_address):
        cls._refresh_serviceable_clients(client_uuid, client_ip_address)

        full_url = 'https://{0}.smoothstreams.tv/{1}/ch{2}q1.stream{3}'.format(
            cls.get_configuration_parameter('SMOOTH_STREAMS_SERVER'),
            cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE'),
            channel_number,
            re.sub(r'/live(.*\?).*&nimblesessionid', r'\1&nimblesessionid', path))

        logger.debug('Proxying request for {0} from {1}/{2} to {3}'.format(path,
                                                                           client_ip_address,
                                                                           client_uuid,
                                                                           full_url))

        smooth_streams_session = cls._get_session_parameter('http_session')

        parsed_url = urllib.parse.urlparse(full_url)

        response = cls._make_http_request(smooth_streams_session.get,
                                          '{0}://{1}{2}'.format(parsed_url.scheme,
                                                                parsed_url.netloc,
                                                                parsed_url.path),
                                          params=dict(urllib.parse.parse_qsl(parsed_url.query)),
                                          headers=smooth_streams_session.headers,
                                          cookies=smooth_streams_session.cookies.get_dict())

        response_status_code = response.status_code
        if response_status_code == requests.codes.OK:
            response_headers = response.headers
            response_text = response.text

            # noinspection PyUnresolvedReferences
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

            return (response_status_code, response_text.replace('.ts?',
                                                                '.ts?channel_number={0}&client_uuid={1}&'.format(
                                                                    channel_number,
                                                                    client_uuid)))
        else:
            logger.error(
                'HTTP error {0} encountered requesting {1} for {2}/{3}'.format(response_status_code,
                                                                               full_url,
                                                                               client_ip_address,
                                                                               client_uuid))

            return (response_status_code, None)

    @classmethod
    def download_playlist_m3u8(cls, path, channel_number, client_uuid, protocol, client_ip_address):
        cls._refresh_serviceable_clients(client_uuid, client_ip_address)
        response_status_code = cls._refresh_session()
        if response_status_code != requests.codes.OK:
            return (response_status_code, None)

        if protocol == 'hls':
            smooth_streams_hash = cls._get_session_parameter('hash')
            smooth_streams_session = cls._get_session_parameter('http_session')

            full_url = 'https://{0}.smoothstreams.tv/{1}/ch{2}q1.stream/playlist.m3u8?wmsAuthSign={3}'.format(
                cls.get_configuration_parameter('SMOOTH_STREAMS_SERVER'),
                cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE'),
                channel_number,
                smooth_streams_hash)

            logger.debug('Proxying request for {0} from {1}/{2} to {3}'.format(path,
                                                                               client_ip_address,
                                                                               client_uuid,
                                                                               full_url))

            parsed_url = urllib.parse.urlparse(full_url)

            response = cls._make_http_request(smooth_streams_session.get,
                                              '{0}://{1}{2}'.format(parsed_url.scheme,
                                                                    parsed_url.netloc,
                                                                    parsed_url.path),
                                              params=dict(urllib.parse.parse_qsl(parsed_url.query)),
                                              headers=smooth_streams_session.headers,
                                              cookies=smooth_streams_session.cookies.get_dict())

            response_status_code = response.status_code
            if response_status_code == requests.codes.OK:
                response_headers = response.headers
                response_text = response.text

                # noinspection PyUnresolvedReferences
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

                return (response_status_code, response_text.replace(
                    'chunks.m3u8?',
                    'chunks.m3u8?channel_number={0}&client_uuid={1}&'.format(channel_number, client_uuid)))
            else:
                logger.error(
                    'HTTP error {0} encountered requesting {1} for {2}/{3}'.format(response_status_code,
                                                                                   full_url,
                                                                                   client_ip_address,
                                                                                   client_uuid))

                return (response_status_code, None)
        elif protocol == 'rtmp':
            smooth_streams_hash = cls._get_session_parameter('hash')

            return (requests.codes.OK,
                    '#EXTM3U\n'
                    '#EXTINF:-1 ,{0}\n'
                    'rtmp://{1}.smoothstreams.tv:3635/{2}?wmsAuthSign={3}/ch{4}q1.stream'.format(
                        cls.get_channel_name(int(channel_number)),
                        cls.get_configuration_parameter('SMOOTH_STREAMS_SERVER'),
                        cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE'),
                        smooth_streams_hash,
                        channel_number))
        else:
            logger.error('protocol: {0} sent in the query string is not supported'.format(protocol))

            return (requests.codes.BAD_REQUEST, None)

    @classmethod
    def download_ts_file(cls, path, channel_number, client_uuid, client_ip_address):
        cls._refresh_serviceable_clients(client_uuid, client_ip_address)

        full_url = 'https://{0}.smoothstreams.tv/{1}/ch{2}q1.stream{3}'.format(
            cls.get_configuration_parameter('SMOOTH_STREAMS_SERVER'),
            cls.get_configuration_parameter('SMOOTH_STREAMS_SERVICE'),
            channel_number,
            re.sub(r'/live(.*\?).*&nimblesessionid', r'\1&nimblesessionid', path))

        logger.debug('Proxying request for {0} from {1}/{2} to {3}'.format(path,
                                                                           client_ip_address,
                                                                           client_uuid,
                                                                           full_url))

        smooth_streams_session = cls._get_session_parameter('http_session')

        parsed_url = urllib.parse.urlparse(full_url)

        response = cls._make_http_request(smooth_streams_session.get,
                                          '{0}://{1}{2}'.format(parsed_url.scheme,
                                                                parsed_url.netloc,
                                                                parsed_url.path),
                                          params=dict(urllib.parse.parse_qsl(parsed_url.query)),
                                          headers=smooth_streams_session.headers,
                                          cookies=smooth_streams_session.cookies.get_dict())

        response_status_code = response.status_code
        if response_status_code == requests.codes.OK:
            response_headers = response.headers
            response_content = response.content

            # noinspection PyUnresolvedReferences
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

            return (response_status_code, response_content)
        else:
            logger.error(
                'HTTP error {0} encountered requesting {1} for {2}/{3}'.format(response_status_code,
                                                                               full_url,
                                                                               client_ip_address,
                                                                               client_uuid))

            return (response_status_code, None)

    @classmethod
    def generate_live_playlist_m3u8(cls, channels_json, protocol):
        try:
            channel_map = {}
            playlist_m3u8 = []
            client_uuid = '{0}'.format(uuid.uuid4())

            for channel_key in sorted(channels_json, key=int):
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
                            cls._configuration['SERVER_HOST'],
                            cls._configuration['SERVER_PORT'],
                            int(channel_number),
                            client_uuid,
                            protocol))
                elif protocol == 'rtmp':
                    response_status_code = cls._refresh_session()
                    if response_status_code != requests.codes.OK:
                        return (response_status_code, None)

                    smooth_streams_hash = cls._get_session_parameter('hash')

                    playlist_m3u8.append(
                        'rtmp://{0}.smoothstreams.tv:3635/{1}?wmsAuthSign={2}/ch{3:02}q1.stream\n'.format(
                            cls._configuration['SMOOTH_STREAMS_SERVER'],
                            cls._configuration['SMOOTH_STREAMS_SERVICE'],
                            smooth_streams_hash,
                            int(channel_number),
                            client_uuid))
                else:
                    logger.error('protocol: {0} sent in the query string is not supported'.format(protocol))

                    return (requests.codes.BAD_REQUEST, None)

            cls._set_channel_map(channel_map)

            playlist_m3u8 = '#EXTM3U x-tvg-url="http://{0}:{1}/epg.xml"\n{2}'.format(
                cls._configuration['SERVER_HOST'],
                cls._configuration['SERVER_PORT'],
                ''.join(playlist_m3u8))

            logger.debug('Generated live playlist.m3u8')

            return (requests.codes.OK, playlist_m3u8)
        except (KeyError, ValueError):
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

    @classmethod
    def generate_vod_playlist_m3u8(cls):
        playlist_m3u8 = []
        client_uuid = '{0}'.format(uuid.uuid4())

        for persistent_recording in cls.get_persistent_recordings():
            playlist_m3u8.append(
                '#EXTINF:-1,{0} - [{1} - {2}]\n'
                'http://{3}:{4}/vod/playlist.m3u8?client_uuid={5}&program_title={6}\n'.format(
                    persistent_recording.program_title,
                    persistent_recording.start_date_time_in_utc.astimezone(get_localzone()).strftime(
                        '%Y-%m-%d %H:%M:%S%z'),
                    persistent_recording.end_date_time_in_utc.astimezone(get_localzone()).strftime(
                        '%Y-%m-%d %H:%M:%S%z'),
                    cls._configuration['SERVER_HOST'],
                    cls._configuration['SERVER_PORT'],
                    client_uuid,
                    urllib.parse.quote(persistent_recording.base_recording_directory)))

        if playlist_m3u8:
            playlist_m3u8 = '#EXTM3U\n{0}'.format(''.join(playlist_m3u8))

            logger.debug('Generated VOD playlist.m3u8')
        else:
            logger.debug('No persistent recordings found. VOD playlist.m3u8 will not be generated')

        return playlist_m3u8

    @classmethod
    def get_active_recordings(cls):
        with cls._active_recordings_lock:
            return tuple(cls._active_recordings)

    @classmethod
    def get_channel_name(cls, channel_number):
        with cls._channel_map_lock:
            return cls._channel_map[channel_number] if channel_number in cls._channel_map else 'Channel {0:02}'.format(
                channel_number)

    @classmethod
    def get_configuration_file_path(cls):
        return cls._configuration_file_path

    @classmethod
    def get_configuration_parameter(cls, parameter_name):
        with cls._configuration_lock:
            return cls._configuration[parameter_name]

    @classmethod
    def get_file_content(cls, file_name):
        do_download_file = False

        with cls._files_map_lock:
            if file_name in cls._files_map:
                if datetime.now(pytz.utc) > cls._files_map[file_name]['next_update_date_time']:
                    logger.info('{0} is stale. Will retrieve it.'.format(file_name))

                    do_download_file = True
                elif not os.path.exists(cls._files_map[file_name]['source']):
                    logger.info('{0} was not found. Will retrieve it.'.format(file_name))

                    do_download_file = True
                else:
                    logger.info('{0} exists and is fresh.'.format(file_name))

                    file_content = None
                    try:
                        with open(cls._files_map[file_name]['source'], 'r') as in_file:
                            file_content = in_file.read()
                    except OSError:
                        do_download_file = True

                    if not do_download_file:
                        return (requests.codes.OK, file_content)
            else:
                logger.info('{0} was never retrieved. Will retrieve it.'.format(file_name))

                do_download_file = True

            if do_download_file:
                return cls._download_file(file_name)

    @classmethod
    def get_persistent_recordings(cls):
        persistent_recordings = []

        recordings_top_level_directory = [recording_top_level_directory for recording_top_level_directory in
                                          os.listdir(cls._recordings_directory_path)
                                          if os.path.isdir(os.path.join(cls._recordings_directory_path,
                                                                        recording_top_level_directory))]
        if recordings_top_level_directory:
            for recording_top_level_directory in recordings_top_level_directory:
                try:
                    recording_top_level_directory_path = os.path.join(cls._recordings_directory_path,
                                                                      recording_top_level_directory,
                                                                      '.MANIFEST')
                    with open(recording_top_level_directory_path, 'r') as in_file:
                        recording_manifest = json.load(in_file)
                        if recording_manifest['status'] == 'Completed':
                            recording = Recording(recording_manifest['channel_number'],
                                                  datetime.strptime(recording_manifest['actual_end_date_time_in_utc'],
                                                                    '%Y-%m-%d %H:%M:%S%z'),
                                                  recording_manifest['program_title'],
                                                  datetime.strptime(recording_manifest['actual_start_date_time_in_utc'],
                                                                    '%Y-%m-%d %H:%M:%S%z'))
                            recording.base_recording_directory = recording_manifest['base_recording_directory']
                            persistent_recordings.append(recording)
                except OSError:
                    logger.error('')

        return persistent_recordings

    @classmethod
    def get_recordings_directory_path(cls):
        return cls._recordings_directory_path

    @classmethod
    def get_scheduled_recordings(cls):
        with cls._scheduled_recordings_lock:
            return tuple(cls._scheduled_recordings)

    @classmethod
    def get_serviceable_client_parameter(cls, client_uuid, parameter_name):
        with cls._serviceable_clients_lock:
            return cls._serviceable_clients[client_uuid][parameter_name]

    @classmethod
    def parse_query_string(cls, path, parameters_default_values_map):
        query_string = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)

        return [
            ''.join(query_string[parameter]) if parameter in query_string else parameters_default_values_map[parameter]
            for parameter in sorted(parameters_default_values_map)]

    @classmethod
    def prepare_response_headers(cls, response_content, content_type):
        return {'Accept-Ranges': 'bytes',
                'Content-Length': '{0}'.format(len(response_content)),
                'Content-Type': content_type
                }

    @classmethod
    def process_path_mapping(cls, path, channel_number, client_uuid, nimble_session_id_in_request,
                             smooth_streams_hash_in_request, client_ip_address):
        smooth_streams_hash = cls._get_session_parameter('hash')

        if smooth_streams_hash_in_request != smooth_streams_hash:
            mapped_nimble_session_id = cls._get_mapped_nimble_session_id(nimble_session_id_in_request)

            if not mapped_nimble_session_id:
                logger.debug('Authorization hash {0} in request from {1}/{2} expired'.format(
                    smooth_streams_hash_in_request,
                    client_ip_address,
                    client_uuid))

                (response_status_code, response_text) = cls.download_playlist_m3u8(path,
                                                                                   channel_number,
                                                                                   client_uuid,
                                                                                   'hls',
                                                                                   client_ip_address)

                if response_status_code == requests.codes.OK:
                    m3u8_obj = m3u8.loads(response_text)

                    path = '/{0}'.format(m3u8_obj.data['playlists'][0]['uri'])
                    (mapped_nimble_session_id,) = cls.parse_query_string(
                        path,
                        {'nimblesessionid': None})

                    logger.debug('Hijacking expired nimble session {0} to new nimble session {1}'.format(
                        nimble_session_id_in_request,
                        mapped_nimble_session_id
                    ))
                    cls._hijack_nimble_session_id(nimble_session_id_in_request, mapped_nimble_session_id)
            else:
                path = '/chunks.m3u8?channel_number={0}&client_uuid={1}&nimblesessionid={2}&&wmsAuthSign={3}'.format(
                    channel_number,
                    client_uuid,
                    mapped_nimble_session_id,
                    smooth_streams_hash)
        return path

    @classmethod
    def read_configuration_file(cls, initial_read=True):
        with cls._configuration_lock:
            configuration = configparser.RawConfigParser()
            configuration.read(cls._configuration_file_path)

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
                error_messages.insert(0, 'Configuration file => {0}'.format(cls._configuration_file_path))
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

                logger.info('{0}ead configuration file {1}\n'
                            'SERVER_HOST             => {2}\n'
                            'SERVER_PORT             => {3}\n'
                            'SMOOTH_STREAMS_SERVICE  => {4}\n'
                            'SMOOTH_STREAMS_SERVER   => {5}\n'
                            'SMOOTH_STREAMS_USERNAME => {6}\n'
                            'SMOOTH_STREAMS_PASSWORD => {7}\n'
                            'SMOOTH_STREAMS_PROTOCOL => {8}\n'
                            'LOGGING_LEVEL           => {9}'.format('R' if initial_read else 'Rer',
                                                                    cls._configuration_file_path,
                                                                    cls._configuration['SERVER_HOST'],
                                                                    cls._configuration['SERVER_PORT'],
                                                                    cls._configuration['SMOOTH_STREAMS_SERVICE'],
                                                                    cls._configuration['SMOOTH_STREAMS_SERVER'],
                                                                    cls._configuration['SMOOTH_STREAMS_USERNAME'],
                                                                    cls._configuration['SMOOTH_STREAMS_PASSWORD'],
                                                                    cls._configuration['SMOOTH_STREAMS_PROTOCOL'],
                                                                    cls._configuration['LOGGING_LEVEL']))

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
        except OSError:
            logger.error('Failed to read {0}'.format(ts_file_path))

        return ts_file_content

    @classmethod
    def read_vod_playlist_m3u8(cls, program_title):
        vod_playlist_m3u8_content = None
        vod_playlist_m3u8_file_path = os.path.join(cls._recordings_directory_path, program_title, 'playlist',
                                                   'playlist.m3u8')

        try:
            with open(vod_playlist_m3u8_file_path, 'r') as in_file:
                vod_playlist_m3u8_content = in_file.read()
        except OSError:
            logger.error('Failed to read {0}'.format(vod_playlist_m3u8_file_path))

        return vod_playlist_m3u8_content

    @classmethod
    def set_logging_level(cls, log_level):
        logger.setLevel(log_level)

        for handler in logger.handlers:
            handler.setLevel(log_level)

    @classmethod
    def set_serviceable_client_parameter(cls, client_uuid, parameter_name, parameter_value):
        with cls._serviceable_clients_lock:
            cls._serviceable_clients[client_uuid][parameter_name] = parameter_value

    @classmethod
    def start(cls, number_of_threads):
        (configuration_file_path, log_file_path, recordings_directory_path) = cls._parse_command_line_arguments()
        cls._configuration_file_path = os.path.abspath(configuration_file_path)
        cls._log_file_path = os.path.abspath(log_file_path)
        cls._recordings_directory_path = os.path.abspath(recordings_directory_path)

        cls._initialize_logging()

        cls.read_configuration_file()
        try:
            cls.set_logging_level(getattr(logging,
                                          cls._configuration['LOGGING_LEVEL'].upper()))
        except AttributeError:
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

        cls._cleanup_shelf()
        cls._load_shelved_settings()
        cls._manage_password()
        cls._refresh_session()
        cls._restart_active_recording()
        cls._set_start_recording_timer()

        smooth_streams_proxy_configuration_event_handler = SmoothStreamsProxyConfigurationEventHandler()
        watchdog_observer = Observer()
        watchdog_observer.schedule(smooth_streams_proxy_configuration_event_handler,
                                   os.path.dirname(cls._configuration_file_path),
                                   recursive=False)
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
