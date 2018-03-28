import base64
import json
import logging.handlers
import pprint
import re
import sys
import traceback
import urllib.parse
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from http.server import HTTPServer
from threading import Thread

import pytz
import requests
from tzlocal import get_localzone

from .constants import VALID_SMOOTH_STREAMS_PROTOCOL_VALUES
from .constants import VERSION
from .enums import SmoothStreamsProxyRecordingStatus
from .exceptions import DuplicateRecordingError
from .exceptions import RecordingNotFoundError
from .proxy import SmoothStreamsProxy
from .recorder import SmoothStreamsProxyRecording
from .utilities import SmoothStreamsProxyUtility
from .validators import SmoothStreamsProxyCerberusValidator

logger = logging.getLogger(__name__)


class SmoothStreamsProxyHTTPRequestHandler(BaseHTTPRequestHandler):
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
            'Response to {0}{1} for {2}\n'
            '[Status Code]\n=============\n{3}\n\n'
            '{4}'
            '{5}'.format(client_ip_address,
                         '/{0}'.format(client_uuid) if client_uuid else '',
                         path,
                         response_status_code,
                         '[Header]\n========\n{0}\n\n'.format('\n'.join(headers)) if headers else '',
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
        client_ip_address = self.client_address[0]
        requested_path_with_query_string = self.path
        requested_url_components = urllib.parse.urlparse(requested_path_with_query_string)
        requested_query_string_parameters = dict(urllib.parse.parse_qsl(requested_url_components.query))
        requested_path_tokens = [requested_path_token.lower()
                                 for requested_path_token in requested_url_components.path[1:].split('/')]
        requested_path_tokens_length = len(requested_path_tokens)
        requested_path_not_found = False

        # noinspection PyBroadException
        try:
            logger.debug('{0} requested from {1}\n'
                         'Request type => {2}'.format(requested_path_with_query_string,
                                                      client_ip_address,
                                                      self.command))

            if requested_path_tokens_length == 2 and \
                    requested_path_tokens[0] == 'recordings' and \
                    re.match('\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z',
                             requested_path_tokens[1]):
                content_length = int(self.headers.get('Content-Length', 0))
                delete_request_body = self.rfile.read(content_length) if content_length else ''

                query_string_parameters_schema = {}
                query_string_parameters_validator = SmoothStreamsProxyCerberusValidator(query_string_parameters_schema)

                if delete_request_body:
                    logger.error(
                        'Error encountered processing request\n'
                        'Source IP      => {0}\n'
                        'Requested path => {1}\n'
                        'Error Title    => Unsupported request body\n'
                        'Error Message  => {2} recordings does not support a request body'.format(
                            client_ip_address,
                            requested_path_with_query_string,
                            self.command))

                    delete_recordings_response = {
                        'errors': [
                            {
                                'status': '{0}'.format(requests.codes.BAD_REQUEST),
                                'title': 'Unsupported request body',
                                'field': None,
                                'developer_message': '{0} recordings does not support a request body'.format(
                                    self.command),
                                'user_message': 'The request is badly formatted'
                            }
                        ]
                    }
                    delete_recordings_response_status_code = requests.codes.BAD_REQUEST
                elif not query_string_parameters_validator.validate(requested_query_string_parameters):
                    logger.error(
                        'Error encountered processing request\n'
                        'Source IP      => {0}\n'
                        'Requested path => {1}\n'
                        'Error Title    => Unsupported query parameter{2}\n'
                        'Error Message  => {3} recordings does not support [\'{4}\'] query parameter{2}'.format(
                            client_ip_address,
                            requested_path_with_query_string,
                            's' if len(query_string_parameters_validator.errors) > 1 else '',
                            self.command,
                            ', '.join(query_string_parameters_validator.errors)))

                    delete_recordings_response = {
                        'errors': [
                            {
                                'status': '{0}'.format(requests.codes.BAD_REQUEST),
                                'title': 'Unsupported query parameter{0}'.format(
                                    's' if len(query_string_parameters_validator.errors) > 1 else ''),
                                'field': list(sorted(query_string_parameters_validator.errors)),
                                'developer_message': '{0} recordings does not support [\'{1}\'] query parameter'
                                                     '{2}'.format(
                                    self.command,
                                    ', '.join(query_string_parameters_validator.errors),
                                    's' if len(query_string_parameters_validator.errors) > 1 else ''),
                                'user_message': 'The request is badly formatted'
                            }
                        ]
                    }
                    delete_recordings_response_status_code = requests.codes.BAD_REQUEST
                else:
                    recording_id = requested_url_components.path[len('/recordings/'):]

                    try:
                        recording = SmoothStreamsProxy.get_recording(recording_id)

                        logger.debug(
                            'Attempting to {0} {1} recording\n'
                            'Channel name      => {2}\n'
                            'Channel number    => {3}\n'
                            'Program title     => {4}\n'
                            'Start date & time => {5}\n'
                            'End date & time   => {6}'.format(
                                'stop'
                                if recording.status == SmoothStreamsProxyRecordingStatus.ACTIVE.value
                                else 'delete',
                                recording.status,
                                recording.channel_name,
                                recording.channel_number,
                                recording.program_title,
                                recording.start_date_time_in_utc.astimezone(
                                    get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                                recording.end_date_time_in_utc.astimezone(
                                    get_localzone()).strftime('%Y-%m-%d %H:%M:%S')))

                        if recording.status == SmoothStreamsProxyRecordingStatus.ACTIVE.value:
                            try:
                                SmoothStreamsProxy.stop_active_recording(recording)
                            except KeyError:
                                raise RecordingNotFoundError
                        elif recording.status == SmoothStreamsProxyRecordingStatus.PERSISTED.value:
                            try:
                                SmoothStreamsProxy.delete_persisted_recording(recording)
                            except OSError:
                                raise RecordingNotFoundError
                        elif recording.status == SmoothStreamsProxyRecordingStatus.SCHEDULED.value:
                            try:
                                SmoothStreamsProxy.delete_scheduled_recording(recording)
                            except ValueError:
                                raise RecordingNotFoundError

                        logger.debug(
                            '{0} {1} recording\n'
                            'Channel name      => {2}\n'
                            'Channel number    => {3}\n'
                            'Program title     => {4}\n'
                            'Start date & time => {5}\n'
                            'End date & time   => {6}'.format(
                                'Stopped'
                                if recording.status == SmoothStreamsProxyRecordingStatus.ACTIVE.value
                                else 'Deleted',
                                recording.status,
                                recording.channel_name,
                                recording.channel_number,
                                recording.program_title,
                                recording.start_date_time_in_utc.astimezone(
                                    get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                                recording.end_date_time_in_utc.astimezone(
                                    get_localzone()).strftime('%Y-%m-%d %H:%M:%S')))

                        delete_recordings_response = {
                            'meta': {
                                'application': 'SmoothStreamsProxy',
                                'version': VERSION
                            }
                        }
                        delete_recordings_response_status_code = requests.codes.OK
                    except RecordingNotFoundError:
                        logger.error(
                            'Error encountered processing request\n'
                            'Source IP      => {0}\n'
                            'Requested path => {1}\n'
                            'Error Title    => Resource not found\n'
                            'Error Message  => Recording with ID {2} does not exist'.format(
                                client_ip_address,
                                requested_path_with_query_string,
                                recording_id))

                        delete_recordings_response = {
                            'errors': [
                                {
                                    'status': '{0}'.format(requests.codes.NOT_FOUND),
                                    'title': 'Resource not found',
                                    'field': None,
                                    'developer_message': 'Recording with ID {0} does not exist'.format(recording_id),
                                    'user_message': 'Requested recording no longer exists'
                                }
                            ]
                        }
                        delete_recordings_response_status_code = requests.codes.NOT_FOUND

                json_api_response = json.dumps(delete_recordings_response, indent=4)
                self._send_http_response(client_ip_address,
                                         None,
                                         requested_path_with_query_string,
                                         delete_recordings_response_status_code,
                                         SmoothStreamsProxyUtility.construct_response_headers(
                                             json_api_response,
                                             'application/vnd.api+json'),
                                         json_api_response)
            else:
                requested_path_not_found = True

            if requested_path_not_found:
                logger.error('HTTP error {0} encountered requesting {1} for {2}'.format(
                    requests.codes.NOT_FOUND,
                    requested_path_with_query_string,
                    client_ip_address))

                self._send_http_response(client_ip_address,
                                         requested_query_string_parameters.get('client_uuid', None),
                                         requested_path_with_query_string,
                                         requests.codes.NOT_FOUND,
                                         SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                              None),
                                         [])
        except Exception:
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

            self._send_http_response(client_ip_address,
                                     requested_query_string_parameters.get('client_uuid', None),
                                     requested_path_with_query_string,
                                     requests.codes.INTERNAL_SERVER_ERROR,
                                     SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                          None),
                                     [])

    # noinspection PyPep8Naming
    def do_GET(self):
        client_ip_address = self.client_address[0]
        requested_path_with_query_string = self.path
        requested_url_components = urllib.parse.urlparse(requested_path_with_query_string)
        requested_query_string_parameters = dict(urllib.parse.parse_qsl(requested_url_components.query))
        requested_path_tokens = [requested_path_token.lower()
                                 for requested_path_token in requested_url_components.path[1:].split('/')]
        requested_path_tokens_length = len(requested_path_tokens)
        requested_path_not_found = False

        # noinspection PyBroadException
        try:
            logger.debug('{0} requested from {1}\n'
                         'Request type => {2}'.format(requested_path_with_query_string,
                                                      client_ip_address,
                                                      self.command))

            if requested_path_tokens[0] == 'live' and requested_path_tokens_length == 2:
                channel_number_parameter_value = requested_query_string_parameters.get('channel_number', None)
                client_uuid_parameter_value = requested_query_string_parameters.get('client_uuid', None)
                nimble_session_id_parameter_value = requested_query_string_parameters.get('nimblesessionid', None)
                number_of_days_parameter_value = requested_query_string_parameters.get('number_of_days', 1)
                protocol_parameter_value = requested_query_string_parameters.get('protocol', None)
                smooth_streams_hash_parameter_value = requested_query_string_parameters.get('wmsAuthSign', None)

                if protocol_parameter_value not in VALID_SMOOTH_STREAMS_PROTOCOL_VALUES:
                    protocol_parameter_value = SmoothStreamsProxy.get_configuration_parameter('SMOOTH_STREAMS_PROTOCOL')

                if requested_path_tokens[1].endswith('.ts'):
                    try:
                        ts_file_content = SmoothStreamsProxy.download_ts_file(client_ip_address,
                                                                              requested_url_components.path,
                                                                              channel_number_parameter_value,
                                                                              client_uuid_parameter_value,
                                                                              nimble_session_id_parameter_value)
                        self._send_http_response(client_ip_address,
                                                 client_uuid_parameter_value,
                                                 requested_path_with_query_string,
                                                 requests.codes.OK,
                                                 SmoothStreamsProxyUtility.construct_response_headers(ts_file_content,
                                                                                                      'video/m2ts'),
                                                 ts_file_content,
                                                 False)
                    except requests.exceptions.HTTPError as e:
                        self._send_http_response(client_ip_address,
                                                 client_uuid_parameter_value,
                                                 requested_path_with_query_string,
                                                 e.response.status_code,
                                                 SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                                      None),
                                                 [])
                elif requested_path_tokens[1] == 'chunks.m3u8':
                    nimble_session_id_parameter_value = SmoothStreamsProxy.map_nimble_session_id(
                        client_ip_address,
                        requested_url_components.path,
                        channel_number_parameter_value,
                        client_uuid_parameter_value,
                        nimble_session_id_parameter_value,
                        smooth_streams_hash_parameter_value)

                    try:
                        playlist_m3u8_content = SmoothStreamsProxy.download_chunks_m3u8(
                            client_ip_address,
                            requested_url_components.path,
                            channel_number_parameter_value,
                            client_uuid_parameter_value,
                            nimble_session_id_parameter_value)

                        self._send_http_response(client_ip_address,
                                                 client_uuid_parameter_value,
                                                 requested_path_with_query_string,
                                                 requests.codes.OK,
                                                 SmoothStreamsProxyUtility.construct_response_headers(
                                                     playlist_m3u8_content,
                                                     'application/vnd.apple.mpegurl'),
                                                 playlist_m3u8_content)
                    except requests.exceptions.HTTPError as e:
                        self._send_http_response(client_ip_address,
                                                 client_uuid_parameter_value,
                                                 requested_path_with_query_string,
                                                 e.response.status_code,
                                                 SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                                      None),
                                                 [])
                elif requested_path_tokens[1] == 'epg.xml':
                    epg_file_name = 'xmltv{0}.xml.gz'.format(number_of_days_parameter_value)

                    try:
                        epg_xml_content = SmoothStreamsProxy.get_file_content(epg_file_name)
                        self._send_http_response(client_ip_address,
                                                 None,
                                                 requested_path_with_query_string,
                                                 requests.codes.OK,
                                                 SmoothStreamsProxyUtility.construct_response_headers(
                                                     epg_xml_content,
                                                     'application/xml'),
                                                 epg_xml_content,
                                                 do_print_content=False)
                    except requests.exceptions.HTTPError as e:
                        self._send_http_response(client_ip_address,
                                                 None,
                                                 requested_path_with_query_string,
                                                 e.response.status_code,
                                                 SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                                      None),
                                                 [])
                elif requested_path_tokens[1] == 'playlist.m3u8':
                    do_generate_playlist_m3u8 = False

                    if requested_query_string_parameters:
                        if channel_number_parameter_value:
                            logger.info('{0} requested from {1}/{2}'.format(
                                SmoothStreamsProxy.get_channel_name(int(channel_number_parameter_value)),
                                client_ip_address,
                                client_uuid_parameter_value))

                            try:
                                playlist_m3u8_content = SmoothStreamsProxy.download_playlist_m3u8(
                                    client_ip_address,
                                    requested_url_components.path,
                                    channel_number_parameter_value,
                                    client_uuid_parameter_value,
                                    protocol_parameter_value)
                                self._send_http_response(client_ip_address,
                                                         client_uuid_parameter_value,
                                                         requested_path_with_query_string,
                                                         requests.codes.OK,
                                                         SmoothStreamsProxyUtility.construct_response_headers(
                                                             playlist_m3u8_content,
                                                             'application/vnd.apple.mpegurl'),
                                                         playlist_m3u8_content)
                            except requests.exceptions.HTTPError as e:
                                self._send_http_response(client_ip_address,
                                                         client_uuid_parameter_value,
                                                         requested_path_with_query_string,
                                                         e.response.status_code,
                                                         SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                                              None),
                                                         [])
                        elif protocol_parameter_value:
                            do_generate_playlist_m3u8 = True
                        else:
                            logger.error('{0} requested from {1}/{2} has an invalid query string'.format(
                                requested_path_with_query_string,
                                client_ip_address,
                                client_uuid_parameter_value))

                            self._send_http_response(client_ip_address,
                                                     client_uuid_parameter_value,
                                                     requested_path_with_query_string,
                                                     requests.codes.BAD_REQUEST,
                                                     SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                                          None),
                                                     [])
                    else:
                        do_generate_playlist_m3u8 = True

                    if do_generate_playlist_m3u8:
                        try:
                            channels_json_content = SmoothStreamsProxy.get_file_content('channels.json')
                            playlist_m3u8_content = SmoothStreamsProxy.generate_live_playlist_m3u8(
                                client_ip_address,
                                json.loads(channels_json_content),
                                protocol_parameter_value)
                            self._send_http_response(client_ip_address,
                                                     None,
                                                     requested_path_with_query_string,
                                                     requests.codes.OK,
                                                     SmoothStreamsProxyUtility.construct_response_headers(
                                                         playlist_m3u8_content,
                                                         'application/vnd.apple.mpegurl'),
                                                     playlist_m3u8_content)
                        except requests.exceptions.HTTPError as e:
                            self._send_http_response(client_ip_address,
                                                     None,
                                                     requested_path_with_query_string,
                                                     e.response.status_code,
                                                     SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                                          None),
                                                     [])
                else:
                    requested_path_not_found = True
            elif requested_path_tokens[0] == 'recordings':
                content_length = int(self.headers.get('Content-Length', 0))
                get_request_body = self.rfile.read(content_length) if content_length else ''

                if get_request_body:
                    logger.error(
                        'Error encountered processing request\n'
                        'Source IP      => {0}\n'
                        'Requested path => {1}\n'
                        'Error Title    => Unsupported request body\n'
                        'Error Message  => {2} recordings does not support a request body'.format(
                            client_ip_address,
                            requested_path_with_query_string,
                            self.command))

                    get_recordings_response = {
                        'errors': [
                            {
                                'status': '{0}'.format(requests.codes.BAD_REQUEST),
                                'title': 'Unsupported request body',
                                'field': None,
                                'developer_message': '{0} recordings does not support a request body'.format(
                                    self.command),
                                'user_message': 'The request is badly formatted'
                            }
                        ]
                    }
                    get_recordings_response_status_code = requests.codes.BAD_REQUEST
                elif requested_path_tokens_length == 1:
                    query_string_parameters_schema = {
                        'status': {
                            'allowed': [SmoothStreamsProxyRecordingStatus.ACTIVE.value,
                                        SmoothStreamsProxyRecordingStatus.PERSISTED.value,
                                        SmoothStreamsProxyRecordingStatus.SCHEDULED.value],
                            'type': 'string'
                        }
                    }
                    query_string_parameters_validator = SmoothStreamsProxyCerberusValidator(
                        query_string_parameters_schema)

                    if not query_string_parameters_validator.validate(requested_query_string_parameters):
                        if [key for key in query_string_parameters_validator.errors if key != 'status']:
                            logger.error(
                                'Error encountered processing request\n'
                                'Source IP      => {0}\n'
                                'Requested path => {1}\n'
                                'Error Title    => Unsupported query parameter{2}\n'
                                'Error Message  => {3} recordings does not support [\'{4}\'] query parameter{2}'.format(
                                    client_ip_address,
                                    requested_path_with_query_string,
                                    's' if len([error_key
                                                for error_key in query_string_parameters_validator.errors
                                                if error_key != 'status']) > 1 else '',
                                    self.command,
                                    ', '.join([error_key
                                               for error_key in query_string_parameters_validator.errors
                                               if error_key != 'status'])))

                            get_recordings_response = {'errors': [
                                {
                                    'status': '{0}'.format(requests.codes.BAD_REQUEST),
                                    'title': 'Unsupported query parameter{0}'.format(
                                        's' if len(query_string_parameters_validator.errors) > 1 else ''),
                                    'field': list(sorted(query_string_parameters_validator.errors)),
                                    'developer_message': '{0} recordings does not support [\'{1}\'] query parameter'
                                                         '{2}'.format(
                                        self.command,
                                        ', '.join([error_key
                                                   for error_key in query_string_parameters_validator.errors
                                                   if error_key != 'status']),
                                        's' if len(
                                            [error_key
                                             for error_key in query_string_parameters_validator.errors
                                             if error_key != 'status']) > 1 else ''),
                                    'user_message': 'The request is badly formatted'
                                }
                            ]}
                            get_recordings_response_status_code = requests.codes.BAD_REQUEST
                        else:
                            logger.error(
                                'Error encountered processing request\n'
                                'Source IP      => {0}\n'
                                'Requested path => {1}\n'
                                'Error Title    => Invalid query parameter value\n'
                                'Error Message  => {2} recordings query parameter [\'status\'] value \'{3}\' '
                                'is not supported'.format(
                                    client_ip_address,
                                    requested_path_with_query_string,
                                    self.command,
                                    requested_query_string_parameters['status']))

                            get_recordings_response = {
                                'errors': [
                                    {
                                        'status': '{0}'.format(requests.codes.UNPROCESSABLE_ENTITY),
                                        'title': 'Invalid query parameter value',
                                        'field': ['status'],
                                        'developer_message': '{0} recordings query parameter [\'status\'] value '
                                                             '\'{1}\' is not supported'.format(
                                            self.command,
                                            requested_query_string_parameters['status']),
                                        'user_message': 'The request is badly formatted'
                                    }
                                ]
                            }
                            get_recordings_response_status_code = requests.codes.UNPROCESSABLE_ENTITY
                    else:
                        get_recordings_response = {
                            'meta': {
                                'application': 'SmoothStreamsProxy',
                                'version': VERSION
                            },
                            'data': []
                        }
                        status = requested_query_string_parameters.get('status', None)

                        for recording in [recording for recording in SmoothStreamsProxy.get_recordings()
                                          if status is None or status == recording.status]:
                            get_recordings_response['data'].append({
                                'type': 'recordings',
                                'id': recording.id,
                                'attributes': {
                                    'channel_name': recording.channel_name,
                                    'channel_number': recording.channel_number,
                                    'end_date_time_in_utc': '{0}'.format(recording.end_date_time_in_utc),
                                    'program_title': recording.program_title,
                                    'start_date_time_in_utc': '{0}'.format(recording.start_date_time_in_utc),
                                    'status': recording.status
                                }
                            })

                        get_recordings_response_status_code = requests.codes.OK
                elif re.match('\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z',
                              requested_path_tokens[1]) and requested_path_tokens_length == 2:
                    query_string_parameters_schema = {}
                    query_string_parameters_validator = SmoothStreamsProxyCerberusValidator(
                        query_string_parameters_schema)

                    if not query_string_parameters_validator.validate(requested_query_string_parameters):
                        logger.error(
                            'Error encountered processing request\n'
                            'Source IP      => {0}\n'
                            'Requested path => {1}\n'
                            'Error Title    => Unsupported query parameter{2}\n'
                            'Error Message  => {3} recordings does not support [\'{4}\'] query parameter{2}'.format(
                                client_ip_address,
                                requested_path_with_query_string,
                                's' if len(query_string_parameters_validator.errors) > 1 else '',
                                self.command,
                                ', '.join(query_string_parameters_validator.errors)))

                        get_recordings_response = {
                            'errors': [
                                {
                                    'status': '{0}'.format(requests.codes.BAD_REQUEST),
                                    'title': 'Unsupported query parameter{0}'.format(
                                        's' if len(query_string_parameters_validator.errors) > 1 else ''),
                                    'field': list(sorted(query_string_parameters_validator.errors)),
                                    'developer_message': '{0} recordings does not support [\'{1}\'] query parameter'
                                                         '{2}'.format(
                                        self.command,
                                        ', '.join(query_string_parameters_validator.errors),
                                        's' if len(query_string_parameters_validator.errors) > 1 else ''),
                                    'user_message': 'The request is badly formatted'
                                }
                            ]
                        }
                        get_recordings_response_status_code = requests.codes.BAD_REQUEST
                    else:
                        recording_id = requested_path_tokens[1]

                        try:
                            recording = SmoothStreamsProxy.get_recording(recording_id)

                            get_recordings_response = {
                                'meta': {
                                    'application': 'SmoothStreamsProxy',
                                    'version': VERSION
                                },
                                'data': {
                                    'type': 'recordings',
                                    'id': recording.id,
                                    'attributes': {
                                        'channel_name': recording.channel_name,
                                        'channel_number': recording.channel_number,
                                        'end_date_time_in_utc': '{0}'.format(recording.end_date_time_in_utc),
                                        'program_title': recording.program_title,
                                        'start_date_time_in_utc': '{0}'.format(recording.start_date_time_in_utc),
                                        'status': recording.status
                                    }
                                }
                            }
                            get_recordings_response_status_code = requests.codes.OK
                        except RecordingNotFoundError:
                            logger.error(
                                'Error encountered processing request\n'
                                'Source IP      => {0}\n'
                                'Requested path => {1}\n'
                                'Error Title    => Resource not found\n'
                                'Error Message  => Recording with ID {2} does not exist'.format(
                                    client_ip_address,
                                    requested_path_with_query_string,
                                    recording_id))

                            get_recordings_response = {
                                'errors': [
                                    {
                                        'status': '{0}'.format(requests.codes.NOT_FOUND),
                                        'title': 'Resource not found',
                                        'field': None,
                                        'developer_message': 'Recording with ID {0} does not exist'.format(
                                            recording_id),
                                        'user_message': 'Requested recording no longer exists'
                                    }
                                ]
                            }
                            get_recordings_response_status_code = requests.codes.NOT_FOUND
                else:
                    requested_path_not_found = True

                if not requested_path_not_found:
                    # noinspection PyUnboundLocalVariable
                    json_api_response = json.dumps(get_recordings_response, indent=4)
                    # noinspection PyUnboundLocalVariable
                    self._send_http_response(client_ip_address,
                                             None,
                                             requested_path_with_query_string,
                                             get_recordings_response_status_code,
                                             SmoothStreamsProxyUtility.construct_response_headers(
                                                 json_api_response,
                                                 'application/vnd.api+json'),
                                             json_api_response)
            elif requested_path_tokens[0] == 'vod' and requested_path_tokens_length == 2:
                client_uuid_parameter_value = requested_query_string_parameters.get('client_uuid', None)
                program_title = requested_query_string_parameters.get('program_title', None)

                if requested_path_tokens[1].endswith('.ts'):
                    ts_file_content = SmoothStreamsProxy.read_ts_file(requested_path_with_query_string, program_title)
                    if ts_file_content:
                        self._send_http_response(client_ip_address,
                                                 client_uuid_parameter_value,
                                                 requested_path_with_query_string,
                                                 requests.codes.OK,
                                                 SmoothStreamsProxyUtility.construct_response_headers(ts_file_content,
                                                                                                      'video/m2ts'),
                                                 ts_file_content,
                                                 False)
                    else:
                        self._send_http_response(client_ip_address,
                                                 client_uuid_parameter_value,
                                                 requested_path_with_query_string,
                                                 requests.codes.NOT_FOUND,
                                                 SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                                      None),
                                                 [])
                elif requested_path_tokens[1] == 'playlist.m3u8':
                    if requested_query_string_parameters:
                        logger.info('{0} requested from {1}/{2}'.format(
                            base64.urlsafe_b64decode(program_title.encode()).decode(),
                            client_ip_address,
                            client_uuid_parameter_value))

                        playlist_m3u8_content = SmoothStreamsProxy.read_vod_playlist_m3u8(program_title)
                        if playlist_m3u8_content:
                            self._send_http_response(client_ip_address,
                                                     client_uuid_parameter_value,
                                                     requested_path_with_query_string,
                                                     requests.codes.OK,
                                                     SmoothStreamsProxyUtility.construct_response_headers(
                                                         playlist_m3u8_content,
                                                         'application/vnd.apple.mpegurl'),
                                                     playlist_m3u8_content)
                        else:
                            self._send_http_response(client_ip_address,
                                                     client_uuid_parameter_value,
                                                     requested_path_with_query_string,
                                                     requests.codes.NOT_FOUND,
                                                     SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                                          None),
                                                     [])
                    else:
                        playlist_m3u8_content = SmoothStreamsProxy.generate_vod_playlist_m3u8(client_ip_address)
                        if playlist_m3u8_content:
                            self._send_http_response(client_ip_address,
                                                     None,
                                                     requested_path_with_query_string,
                                                     requests.codes.OK,
                                                     SmoothStreamsProxyUtility.construct_response_headers(
                                                         playlist_m3u8_content,
                                                         'application/vnd.apple.mpegurl'),
                                                     playlist_m3u8_content)
                        else:
                            self._send_http_response(client_ip_address,
                                                     None,
                                                     requested_path_with_query_string,
                                                     requests.codes.NOT_FOUND,
                                                     SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                                          None),
                                                     [])
                else:
                    requested_path_not_found = True
            else:
                requested_path_not_found = True

            if requested_path_not_found:
                logger.error('HTTP error {0} encountered requesting {1} for {2}'.format(
                    requests.codes.NOT_FOUND,
                    requested_path_with_query_string,
                    client_ip_address))

                self._send_http_response(client_ip_address,
                                         requested_query_string_parameters.get('client_uuid', None),
                                         requested_path_with_query_string,
                                         requests.codes.NOT_FOUND,
                                         SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                              None),
                                         [])
        except Exception:
            (status, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(status, value_, traceback_)))

            self._send_http_response(client_ip_address,
                                     requested_query_string_parameters.get('client_uuid', None),
                                     requested_path_with_query_string,
                                     requests.codes.INTERNAL_SERVER_ERROR,
                                     SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                          None),
                                     [])

    # noinspection PyPep8Naming
    def do_OPTIONS(self):
        client_ip_address = self.client_address[0]
        requested_path_with_query_string = self.path
        requested_url_components = urllib.parse.urlparse(requested_path_with_query_string)
        requested_query_string_parameters = dict(urllib.parse.parse_qsl(requested_url_components.query))

        self._send_http_response(client_ip_address,
                                 requested_query_string_parameters.get('client_uuid', None),
                                 requested_path_with_query_string,
                                 requests.codes.OK,
                                 SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                      None),
                                 [])

    # noinspection PyPep8Naming
    def do_POST(self):
        client_ip_address = self.client_address[0]
        requested_path_with_query_string = self.path
        requested_url_components = urllib.parse.urlparse(requested_path_with_query_string)
        requested_query_string_parameters = dict(urllib.parse.parse_qsl(requested_url_components.query))
        requested_path_tokens = [requested_path_token.lower()
                                 for requested_path_token in requested_url_components.path[1:].split('/')]
        requested_path_tokens_length = len(requested_path_tokens)
        requested_path_not_found = False

        # noinspection PyBroadException
        try:
            logger.debug('{0} requested from {1}\n'
                         'Request type => {2}'.format(requested_path_with_query_string,
                                                      client_ip_address,
                                                      self.command))
            if requested_path_tokens_length == 1 and requested_path_tokens[0] == 'recordings':
                content_length = int(self.headers.get('Content-Length', 0))
                invalid_post_request_body = False

                try:
                    post_request_body = json.loads(self.rfile.read(content_length)) if content_length else {}
                except json.JSONDecodeError:
                    invalid_post_request_body = True

                query_string_parameters_schema = {}
                query_string_parameters_validator = SmoothStreamsProxyCerberusValidator(query_string_parameters_schema)

                post_request_body_schema = {
                    'data': {
                        'required': True,
                        'schema': {
                            'type': {
                                'allowed': ['recordings'],
                                'required': True,
                                'type': 'string'
                            },
                            'attributes': {
                                'required': True,
                                'schema': {
                                    'channel_number': {
                                        'is_channel_number_valid': True,
                                        'required': True,
                                        'type': 'string'
                                    },
                                    'end_date_time_in_utc': {
                                        'is_end_date_time_after_start_date_time': 'start_date_time_in_utc',
                                        'is_end_date_time_in_the_future': True,
                                        'required': True,
                                        'type': 'datetime_string'
                                    },
                                    'program_title': {
                                        'required': True,
                                        'type': 'string'
                                    },
                                    'start_date_time_in_utc': {
                                        'required': True,
                                        'type': 'datetime_string'
                                    }
                                },
                                'type': 'dict'
                            }
                        },
                        'type': 'dict'
                    }
                }
                post_request_body_validator = SmoothStreamsProxyCerberusValidator(post_request_body_schema)

                if invalid_post_request_body:
                    logger.error(
                        'Error encountered processing request\n'
                        'Source IP      => {0}\n'
                        'Requested path => {1}\n'
                        'Error Title    => Invalid request body\n'
                        'Error Message  => Request body is not a valid JSON document'.format(
                            client_ip_address,
                            requested_path_with_query_string))

                    post_recordings_response = {
                        'errors': [
                            {
                                'status': '{0}'.format(requests.codes.BAD_REQUEST),
                                'title': 'Invalid request body',
                                'field': None,
                                'developer_message': 'Request body is not a valid JSON document'.format(self.command),
                                'user_message': 'The request is badly formatted'
                            }
                        ]
                    }
                    post_recordings_response_status_code = requests.codes.BAD_REQUEST
                elif not query_string_parameters_validator.validate(requested_query_string_parameters):
                    logger.error(
                        'Error encountered processing request\n'
                        'Source IP      => {0}\n'
                        'Requested path => {1}\n'
                        'Error Title    => Unsupported query parameter{2}\n'
                        'Error Message  => {3} recordings/{{id}} does not support [\'{4}\'] query parameter{2}'.format(
                            client_ip_address,
                            requested_path_with_query_string,
                            's' if len(query_string_parameters_validator.errors) > 1 else '',
                            self.command,
                            ', '.join(query_string_parameters_validator.errors)))

                    post_recordings_response = {
                        'errors': [
                            {
                                'status': '{0}'.format(requests.codes.BAD_REQUEST),
                                'title': 'Unsupported query parameter{0}'.format(
                                    's' if len(query_string_parameters_validator.errors) > 1 else ''),
                                'field': list(sorted(query_string_parameters_validator.errors)),
                                'developer_message': '{0} recordings does not support [\'{1}\'] query parameter'
                                                     '{2}'.format(
                                    self.command,
                                    ', '.join(query_string_parameters_validator.errors),
                                    's' if len(query_string_parameters_validator.errors) > 1 else ''),
                                'user_message': 'The request is badly formatted'
                            }
                        ]
                    }
                    post_recordings_response_status_code = requests.codes.BAD_REQUEST
                elif not post_request_body_validator.validate(post_request_body):
                    missing_required_fields = [match.group().replace(
                        '\'', '') for match in re.finditer(
                        '(\'[^{,\[]+\')(?=: \[\'required field\'\])',
                        '{0}'.format(post_request_body_validator.errors))]

                    included_unknown_fields = [match.group().replace(
                        '\'', '') for match in re.finditer(
                        '(\'[^{,\[]+\')(?=: \[\'unknown field\'\])',
                        '{0}'.format(post_request_body_validator.errors))]

                    incorrect_type_fields = [match.group().replace(
                        '\'', '') for match in re.finditer(
                        '(\'[^{,\[]+\')(?=: \[\'must be of (datetime_string|string) type\'\])',
                        '{0}'.format(post_request_body_validator.errors))]

                    invalid_type_value = [match.group().replace(
                        '\'', '') for match in re.finditer(
                        '(\'[^{,\[]+\')(?=: \[\'unallowed value .*\'\])',
                        '{0}'.format(post_request_body_validator.errors))]

                    invalid_channel_number = [match.group().replace(
                        '\'', '') for match in re.finditer(
                        '(\'[^{,\[]+\')(?=: \[\'must be between [0-9]{2} and [0-9]{2,4}\'\])',
                        '{0}'.format(post_request_body_validator.errors))]

                    invalid_end_date_time_in_the_future = [match.group().replace(
                        '\'', '') for match in re.finditer(
                        '(\'[^{,\[]+\')(?=: \[\'must be later than now\'\])',
                        '{0}'.format(post_request_body_validator.errors))]

                    invalid_end_date_time_after_start_date_time = [match.group().replace(
                        '\'', '') for match in re.finditer(
                        '(\'[^{,\[]+\')(?=: \[\'must be later than start_date_time_in_utc\'\])',
                        '{0}'.format(post_request_body_validator.errors))]

                    if missing_required_fields or included_unknown_fields:
                        logger.error(
                            'Error encountered processing request\n'
                            'Source IP      => {0}\n'
                            'Requested path => {1}\n'
                            'Post Data      => {2}\n'
                            'Error Title    => Invalid resource creation request\n'
                            'Error Message  => Request body {3}'.format(
                                client_ip_address,
                                requested_path_with_query_string,
                                pprint.pformat(post_request_body, indent=4),
                                'is missing mandatory field{0} {1}'.format(
                                    's' if len(missing_required_fields) > 1 else '',
                                    missing_required_fields) if missing_required_fields else
                                'includes unknown field{0} {1}'.format(
                                    's' if len(included_unknown_fields) > 1 else '',
                                    included_unknown_fields)))

                        post_recordings_response = {
                            'errors': [
                                {
                                    'status': '{0}'.format(requests.codes.BAD_REQUEST),
                                    'title': 'Invalid resource creation request',
                                    'field': '{0}'.format(missing_required_fields if missing_required_fields
                                                          else included_unknown_fields),
                                    'developer_message': 'Request body {0}'.format(
                                        'is missing mandatory field{0} {1}'.format(
                                            's' if len(missing_required_fields) > 1 else '',
                                            missing_required_fields) if missing_required_fields else
                                        'includes unknown field{0} {1}'.format(
                                            's' if len(included_unknown_fields) > 1 else '',
                                            included_unknown_fields)),
                                    'user_message': 'The request is badly formatted'
                                }
                            ]
                        }
                        post_recordings_response_status_code = requests.codes.BAD_REQUEST
                    elif incorrect_type_fields or invalid_type_value or invalid_channel_number or \
                            invalid_end_date_time_in_the_future or invalid_end_date_time_after_start_date_time:
                        field = None
                        developer_message = None
                        user_message = None

                        if incorrect_type_fields:
                            field = incorrect_type_fields
                            developer_message = 'Request body includes field{0} with invalid type {1}'.format(
                                's' if len(incorrect_type_fields) > 1 else '',
                                incorrect_type_fields)
                            user_message = 'The request is badly formatted'
                        elif invalid_type_value == ['type']:
                            field = invalid_type_value
                            developer_message = '[\'type\'] must be recordings'
                            user_message = 'The request is badly formatted'
                        elif invalid_channel_number == ['channel_number']:
                            field = invalid_channel_number
                            developer_message = '[\'channel_number\'] {0}'.format(
                                post_request_body_validator.errors['data'][0]['attributes'][0]['channel_number'][0])
                            user_message = 'The requested channel does not exist'
                        elif invalid_end_date_time_in_the_future == ['end_date_time_in_utc']:
                            field = invalid_end_date_time_in_the_future
                            developer_message = '[\'end_date_time_in_utc\'] must be later than now'
                            user_message = 'The requested recording is in the past'
                        elif invalid_end_date_time_after_start_date_time == ['end_date_time_in_utc']:
                            field = invalid_end_date_time_after_start_date_time
                            developer_message = '[\'end_date_time_in_utc\'] must be later than ' \
                                                '[\'start_date_time_in_utc\']'
                            user_message = 'The request is badly formatted'
                        
                        logger.error(
                            'Error encountered processing request\n'
                            'Source IP      => {0}\n'
                            'Requested path => {1}\n'
                            'Post Data      => {2}\n'
                            'Error Title    => Invalid resource creation request\n'
                            'Error Message  => {3}'.format(
                                client_ip_address,
                                requested_path_with_query_string,
                                pprint.pformat(post_request_body, indent=4),
                                developer_message))

                        post_recordings_response = {
                            'errors': [
                                {
                                    'status': '{0}'.format(requests.codes.UNPROCESSABLE_ENTITY),
                                    'title': 'Invalid resource creation request',
                                    'field': field,
                                    'developer_message': '{0}'.format(developer_message),
                                    'user_message': '{0}'.format(user_message)
                                }
                            ]
                        }
                        post_recordings_response_status_code = requests.codes.UNPROCESSABLE_ENTITY
                    else:
                        logger.error(
                            'Error encountered processing request\n'
                            'Source IP      => {0}\n'
                            'Requested path => {1}\n'
                            'Post Data      => {2}\n'
                            'Error Title    => Invalid resource creation request\n'
                            'Error Message  => Unexpected error'.format(
                                client_ip_address,
                                requested_path_with_query_string,
                                pprint.pformat(post_request_body, indent=4)))

                        post_recordings_response = {
                            'errors': [
                                {
                                    'status': '{0}'.format(requests.codes.UNPROCESSABLE_ENTITY),
                                    'title': 'Invalid resource creation request',
                                    'field': None,
                                    'developer_message': 'Unexpected error',
                                    'user_message': 'The request is badly formatted'
                                }
                            ]
                        }
                        post_recordings_response_status_code = requests.codes.UNPROCESSABLE_ENTITY
                else:
                    channel_name = SmoothStreamsProxy.get_channel_name(
                        int(post_request_body['data']['attributes']['channel_number']))
                    channel_number = post_request_body['data']['attributes']['channel_number']
                    end_date_time_in_utc = datetime.strptime(
                        post_request_body['data']['attributes']['end_date_time_in_utc'],
                        '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
                    id_ = '{0}'.format(uuid.uuid4())
                    program_title = post_request_body['data']['attributes']['program_title']
                    start_date_time_in_utc = datetime.strptime(
                        post_request_body['data']['attributes']['start_date_time_in_utc'],
                        '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)

                    recording = SmoothStreamsProxyRecording(channel_name,
                                                            channel_number,
                                                            end_date_time_in_utc,
                                                            id_,
                                                            program_title,
                                                            start_date_time_in_utc,
                                                            SmoothStreamsProxyRecordingStatus.SCHEDULED.value)

                    try:
                        SmoothStreamsProxy.add_scheduled_recording(recording)

                        logger.info(
                            'Scheduled recording\n'
                            'Channel name      => {0}\n'
                            'Channel number    => {1}\n'
                            'Program title     => {2}\n'
                            'Start date & time => {3}\n'
                            'End date & time   => {4}'.format(channel_name,
                                                              channel_number,
                                                              program_title,
                                                              start_date_time_in_utc.astimezone(
                                                                  get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                                                              end_date_time_in_utc.astimezone(
                                                                  get_localzone()).strftime('%Y-%m-%d %H:%M:%S')))

                        post_recordings_response = {
                            'meta': {
                                'application': 'SmoothStreamsProxy',
                                'version': VERSION
                            },
                            'data': {
                                'type': 'recordings',
                                'id': id_,
                                'attributes': {
                                    'channel_name': channel_name,
                                    'channel_number': channel_number,
                                    'end_date_time_in_utc': '{0}'.format(end_date_time_in_utc),
                                    'program_title': program_title,
                                    'start_date_time_in_utc': '{0}'.format(start_date_time_in_utc),
                                    'status': 'scheduled'
                                }
                            }
                        }
                        post_recordings_response_status_code = requests.codes.CREATED
                    except DuplicateRecordingError:
                        logger.error(
                            'Error encountered processing request\n'
                            'Source IP      => {0}\n'
                            'Requested path => {1}\n'
                            'Post Data      => {2}\n'
                            'Error Title    => Duplicate resource\n'
                            'Error Message  => Recording already scheduled'.format(
                                client_ip_address,
                                requested_path_with_query_string,
                                pprint.pformat(post_request_body, indent=4)))

                        post_recordings_response = {
                            'errors': [
                                {
                                    'status': '{0}'.format(requests.codes.CONFLICT),
                                    'field': None,
                                    'title': 'Duplicate resource',
                                    'developer_message': 'Recording already scheduled',
                                    'user_message': 'The recording is already scheduled'
                                }
                            ]
                        }
                        post_recordings_response_status_code = requests.codes.CONFLICT

                json_api_response = json.dumps(post_recordings_response, indent=4)
                self._send_http_response(client_ip_address,
                                         None,
                                         requested_path_with_query_string,
                                         post_recordings_response_status_code,
                                         SmoothStreamsProxyUtility.construct_response_headers(
                                             json_api_response,
                                             'application/vnd.api+json'),
                                         json_api_response)
            else:
                requested_path_not_found = True

            if requested_path_not_found:
                logger.error('HTTP error {0} encountered requesting {1} for {2}'.format(
                    requests.codes.NOT_FOUND,
                    requested_path_with_query_string,
                    client_ip_address))
                self._send_http_response(client_ip_address,
                                         requested_query_string_parameters.get('client_uuid', None),
                                         requested_path_with_query_string,
                                         requests.codes.NOT_FOUND,
                                         SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                              None),
                                         [])
        except Exception:
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

            self._send_http_response(client_ip_address,
                                     requested_query_string_parameters.get('client_uuid', None),
                                     requested_path_with_query_string,
                                     requests.codes.INTERNAL_SERVER_ERROR,
                                     SmoothStreamsProxyUtility.construct_response_headers(None,
                                                                                          None),
                                     [])

    def log_message(self, format_, *args):
        return


class SmoothStreamsProxyHTTPRequestHandlerThread(Thread):
    def __init__(self, server_address, server_socket):
        Thread.__init__(self)

        self.server_address = server_address
        self.server_socket = server_socket
        self.server_close = lambda self_: None

        self._smooth_streams_proxy_http_server = SmoothStreamsProxyHTTPServer(self.server_address,
                                                                              SmoothStreamsProxyHTTPRequestHandler,
                                                                              False)

        self.daemon = True
        self.start()

    def run(self):
        self._smooth_streams_proxy_http_server.socket = self.server_socket
        self._smooth_streams_proxy_http_server.server_bind = self.server_close

        self._smooth_streams_proxy_http_server.serve_forever()

    def stop(self):
        self._smooth_streams_proxy_http_server.shutdown()


class SmoothStreamsProxyHTTPServer(HTTPServer):
    def __init__(self, server_address, request_handler, context):
        HTTPServer.__init__(self, server_address, request_handler, context)
