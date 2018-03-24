import base64
import json
import logging.handlers
import math
import os
import re
import sys
import time
import traceback
import urllib.parse
import uuid
from datetime import datetime
from threading import Event
from threading import Thread
from threading import Timer

import m3u8
import pytz
import requests
from tzlocal import get_localzone

from .proxy import SmoothStreamsProxy

logger = logging.getLogger(__name__)


class SmoothStreamsProxyRecording():
    __slots__ = ['_base_recording_directory', '_channel_name', '_channel_number', '_end_date_time_in_utc', '_id',
                 '_program_title', '_start_date_time_in_utc', '_status']

    def __init__(self, channel_name, channel_number, end_date_time_in_utc, id_, program_title, start_date_time_in_utc,
                 status):
        self._base_recording_directory = None
        self._channel_name = channel_name
        self._channel_number = channel_number
        self._end_date_time_in_utc = end_date_time_in_utc
        self._id = id_
        self._program_title = program_title
        self._start_date_time_in_utc = start_date_time_in_utc
        self._status = status

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.channel_number, self._end_date_time_in_utc, self._start_date_time_in_utc) == (
                other._channel_number, other._end_date_time_in_utc, other._start_date_time_in_utc)
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
    def channel_name(self):
        return self._channel_name

    @property
    def channel_number(self):
        return self._channel_number

    @property
    def end_date_time_in_utc(self):
        return self._end_date_time_in_utc

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id_):
        self._id = id_

    @property
    def program_title(self):
        return self._program_title

    @property
    def start_date_time_in_utc(self):
        return self._start_date_time_in_utc

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status


class SmoothStreamsProxyRecordingThread(Thread):
    def __init__(self, recording):
        Thread.__init__(self)

        self._id = uuid.uuid4()
        self._recording = recording
        self._recording_directory_path = None

        self._stop_recording_event = Event()
        self._stop_recording_timer = Timer(
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
                logger.debug('Creating recording directory tree for {0}\n'
                             'Path => {1}'.format(self._recording.program_title, recording_directory_path))

                try:
                    os.makedirs(recording_directory_path)
                    os.makedirs(os.path.join(recording_directory_path, 'playlist'))
                    os.makedirs(os.path.join(recording_directory_path, 'segments'))

                    did_make_directory = True
                    self._recording_directory_path = recording_directory_path
                    self._recording.base_recording_directory = os.path.split(recording_directory_path)[-1]

                    logger.debug('Created recording directory tree for {0}\n'
                                 'Path => {1}'.format(self._recording.program_title, recording_directory_path))
                except OSError:
                    logger.error('Failed to create recording directory tree for {0}\n'
                                 'Path => {1}'.format(self._recording.program_title, recording_directory_path))

                    recording_directory_suffix_counter += 1
                    recording_directory_suffix = '_{0}'.format(recording_directory_suffix_counter)

    def _save_manifest_file(self, actual_end_date_time_in_utc, actual_start_date_time_in_utc, id_, playlist_file,
                            status):
        manifest_file_path = os.path.join(self._recording_directory_path, '.MANIFEST')

        try:
            with open(manifest_file_path, 'w') as out_file:
                logger.info(json.dumps(actual_start_date_time_in_utc))
                json.dump(
                    {'actual_end_date_time_in_utc': actual_end_date_time_in_utc,
                     'actual_start_date_time_in_utc': actual_start_date_time_in_utc,
                     'channel_name': self._recording.channel_name,
                     'base_recording_directory': self._recording.base_recording_directory,
                     'channel_number': self._recording.channel_number,
                     'id': id_,
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

                logger.debug('Saved .MANIFEST\n'
                             'Path => {0}'.format(manifest_file_path))
        except OSError:
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

    def _save_playlist_file(self, playlist_file_name, playlist_file_content):
        playlist_file_path = os.path.join(self._recording_directory_path, 'playlist', playlist_file_name)

        try:
            with open(playlist_file_path, 'w') as out_file:
                out_file.write(playlist_file_content)

                logger.debug('Saved playlist\n'
                             'Path => {0}'.format(playlist_file_path))
        except OSError:
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

    def _save_segment_file(self, segment_file_name, segments_file_content):
        segment_file_path = os.path.join(self._recording_directory_path, 'segments', segment_file_name)

        try:
            with open(segment_file_path, 'wb') as out_file:
                out_file.write(segments_file_content)

                logger.debug('Saved segment\n'
                             'Path => {0}'.format(segment_file_path))
        except OSError:
            (type_, value_, traceback_) = sys.exc_info()
            logger.error('\n'.join(traceback.format_exception(type_, value_, traceback_)))

    def _set_stop_recording_event(self):
        self._stop_recording_event.set()

        logger.info('Stopping recording\n'
                    'Channel name      => {0}\n'
                    'Channel number    => {1}\n'
                    'Program title     => {2}\n'
                    'Start date & time => {3}\n'
                    'End date & time   => {4}'.format(self._recording.channel_name,
                                                      self._recording.channel_number,
                                                      self._recording.program_title,
                                                      self._recording.start_date_time_in_utc.astimezone(
                                                          get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                                                      self._recording.end_date_time_in_utc.astimezone(
                                                          get_localzone()).strftime('%Y-%m-%d %H:%M:%S')))

    def force_stop(self):
        self._set_stop_recording_event()

    def run(self):
        logger.info('Starting recording\n'
                    'Channel name      => {0}\n'
                    'Channel number    => {1}\n'
                    'Program title     => {2}\n'
                    'Start date & time => {3}\n'
                    'End date & time   => {4}'.format(self._recording.channel_name,
                                                      self._recording.channel_number,
                                                      self._recording.program_title,
                                                      self._recording.start_date_time_in_utc.astimezone(
                                                          get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                                                      self._recording.end_date_time_in_utc.astimezone(
                                                          get_localzone()).strftime('%Y-%m-%d %H:%M:%S')))
        actual_start_date_time_in_utc = datetime.now(pytz.utc)

        self._create_recording_directory_tree()
        persisted_recording_id = '{0}'.format(uuid.uuid4())
        self._save_manifest_file(None,
                                 actual_start_date_time_in_utc.strftime('%Y-%m-%d %H:%M:%S%z'),
                                 persisted_recording_id,
                                 None,
                                 'Started')

        for number_of_times_attempted_to_download_playlist_m3u8 in range(1, 11):
            try:
                # <editor-fold desc="Download playlist.m3u8">
                playlist_m3u8_content = SmoothStreamsProxy.download_playlist_m3u8('127.0.0.1',
                                                                                  '/live/playlist.m3u8',
                                                                                  self._recording.channel_number,
                                                                                  self._id,
                                                                                  'hls')
                # </editor-fold>

                self._save_manifest_file(None,
                                         actual_start_date_time_in_utc.strftime('%Y-%m-%d %H:%M:%S%z'),
                                         persisted_recording_id,
                                         None,
                                         'In Progress')

                playlist_m3u8_object = m3u8.loads(playlist_m3u8_content)
                chunks_url = '/live/{0}'.format(playlist_m3u8_object.data['playlists'][0]['uri'])

                break
            except requests.exceptions.HTTPError:
                time_to_sleep_before_next_attempt = math.ceil(
                    number_of_times_attempted_to_download_playlist_m3u8 / 5) * 5

                logger.error('Attempt #{0}\n'
                             'Failed to download playlist.m3u8\n'
                             'Will try again in {1} seconds'.format(number_of_times_attempted_to_download_playlist_m3u8,
                                                                    time_to_sleep_before_next_attempt))

                time.sleep(time_to_sleep_before_next_attempt)
        else:
            logger.error('Exhausted attempts to download playlist.m3u8')

            logger.info('Canceling recording\n'
                        'Channel name      => {0}\n'
                        'Channel number    => {1}\n'
                        'Program title     => {2}\n'
                        'Start date & time => {3}\n'
                        'End date & time   => {4}'.format(self._recording.channel_name,
                                                          self._recording.channel_number,
                                                          self._recording.program_title,
                                                          self._recording.start_date_time_in_utc.astimezone(
                                                              get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                                                          self._recording.end_date_time_in_utc.astimezone(
                                                              get_localzone()).strftime('%Y-%m-%d %H:%M:%S')))

            self._save_manifest_file(datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S%z'),
                                     actual_start_date_time_in_utc.strftime('%Y-%m-%d %H:%M:%S%z'),
                                     persisted_recording_id,
                                     None,
                                     'Canceled')

            return

        vod_playlist_m3u8_object = None
        downloaded_segment_file_names = []

        while not self._stop_recording_event.is_set():
            try:
                # <editor-fold desc="Download chunks.m3u8">
                chunks_url_components = urllib.parse.urlparse(chunks_url)
                chunks_query_string_parameters = dict(urllib.parse.parse_qsl(chunks_url_components.query))

                channel_number_parameter_value = chunks_query_string_parameters.get('channel_number', None)
                client_uuid_parameter_value = chunks_query_string_parameters.get('client_uuid', None)
                nimble_session_id_parameter_value = chunks_query_string_parameters.get('nimblesessionid', None)
                smooth_streams_hash_parameter_value = chunks_query_string_parameters.get('wmsAuthSign', None)

                nimble_session_id_parameter_value = SmoothStreamsProxy.map_nimble_session_id(
                    '127.0.0.1',
                    chunks_url_components.path,
                    channel_number_parameter_value,
                    client_uuid_parameter_value,
                    nimble_session_id_parameter_value,
                    smooth_streams_hash_parameter_value)

                chunks_m3u8_content = SmoothStreamsProxy.download_chunks_m3u8('127.0.0.1',
                                                                              chunks_url_components.path,
                                                                              channel_number_parameter_value,
                                                                              client_uuid_parameter_value,
                                                                              nimble_session_id_parameter_value)
                # </editor-fold>
                chunks_m3u8_download_date_time_in_utc = datetime.now(pytz.utc)
                chunks_m3u8_total_duration = 0
                chunks_m3u8_object = m3u8.loads(chunks_m3u8_content)

                if not vod_playlist_m3u8_object:
                    vod_playlist_m3u8_object = chunks_m3u8_object

                indices_of_skipped_segments = []
                for (segment_index, segment) in enumerate(chunks_m3u8_object.segments):
                    segment_url = '/live/{0}'.format(segment.uri)
                    segment_url_components = urllib.parse.urlparse(segment_url)
                    segment_query_string_parameters = dict(urllib.parse.parse_qsl(segment_url_components.query))
                    segment_file_name = re.sub(r'(/.*)?(/)(.*\.ts)', r'\3', segment_url_components.path)

                    chunks_m3u8_total_duration += segment.duration

                    if segment_file_name not in downloaded_segment_file_names:
                        try:
                            # <editor-fold desc="Download ts file">
                            channel_number_parameter_value = segment_query_string_parameters.get('channel_number', None)
                            client_uuid_parameter_value = segment_query_string_parameters.get('client_uuid', None)
                            nimble_session_id_parameter_value = segment_query_string_parameters.get('nimblesessionid',
                                                                                                    None)

                            ts_file_content = SmoothStreamsProxy.download_ts_file('127.0.0.1',
                                                                                  segment_url_components.path,
                                                                                  channel_number_parameter_value,
                                                                                  client_uuid_parameter_value,
                                                                                  nimble_session_id_parameter_value)
                            # </editor-fold>
                            logger.debug('Downloaded segment\n'
                                         'Segment => {0}'.format(segment_file_name))

                            downloaded_segment_file_names.append(segment_file_name)
                            self._save_segment_file(segment_file_name, ts_file_content)

                            segment.uri = '{0}?client_uuid={1}&program_title={2}'.format(
                                segment_file_name,
                                client_uuid_parameter_value,
                                urllib.parse.quote(self._recording.base_recording_directory))

                            if segment not in vod_playlist_m3u8_object.segments:
                                vod_playlist_m3u8_object.segments.append(segment)
                        except requests.exceptions.HTTPError:
                            logger.error('Failed to download segment\n'
                                         'Segment => {0}'.format(segment_file_name))
                    else:
                        logger.debug('Skipped segment since it was already downloaded\n'
                                     'Segment => {0} '.format(segment_file_name))

                        indices_of_skipped_segments.append(segment_index)

                for segment_index_to_delete in indices_of_skipped_segments:
                    del chunks_m3u8_object.segments[segment_index_to_delete]
            except requests.exceptions.HTTPError:
                logger.error('Failed to download chunks.m3u8')

                return

            current_date_time_in_utc = datetime.now(pytz.utc)
            wait_duration = chunks_m3u8_total_duration - (
                    current_date_time_in_utc - chunks_m3u8_download_date_time_in_utc).total_seconds()
            if wait_duration > 0:
                self._stop_recording_event.wait(wait_duration)

        if vod_playlist_m3u8_object:
            vod_playlist_m3u8_object.playlist_type = 'VOD'
            self._save_playlist_file('playlist.m3u8', '{0}\n'
                                                      '{1}'.format(vod_playlist_m3u8_object.dumps(), '#EXT-X-ENDLIST'))

        self._save_manifest_file(datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S%z'),
                                 actual_start_date_time_in_utc.strftime('%Y-%m-%d %H:%M:%S%z'),
                                 persisted_recording_id,
                                 'playlist.m3u8',
                                 'Completed')

        SmoothStreamsProxy.delete_active_recording(self._recording)

        logger.info('Finished recording\n'
                    'Channel name      => {0}\n'
                    'Channel number    => {1}\n'
                    'Program title     => {2}\n'
                    'Start date & time => {3}\n'
                    'End date & time   => {4}'.format(self._recording.channel_name,
                                                      self._recording.channel_number,
                                                      self._recording.program_title,
                                                      self._recording.start_date_time_in_utc.astimezone(
                                                          get_localzone()).strftime('%Y-%m-%d %H:%M:%S'),
                                                      self._recording.end_date_time_in_utc.astimezone(
                                                          get_localzone()).strftime('%Y-%m-%d %H:%M:%S')))
