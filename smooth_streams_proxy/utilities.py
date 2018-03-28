import base64
import binascii
import json
import logging
import logging.handlers
import pprint
import re
import socket
import sys
import traceback
from argparse import ArgumentParser

import requests
from cryptography.fernet import Fernet

from .enums import SmoothStreamsProxyPasswordState
from .formatters import SmoothStreamsProxyMultiLineFormatter

TRACE = 5

logger = logging.getLogger(__name__)


class SmoothStreamsProxyUtility():
    @classmethod
    def construct_response_headers(cls, response_content, content_type):
        response_headers = {
            'Accept-Ranges': 'bytes',
            'Access-Control-Allow-Methods': 'DELETE, GET, OPTIONS, POST',
            'Access-Control-Allow-Origin': '*'
        }

        if response_content and content_type:
            response_headers['Content-Length'] = '{0}'.format(len(response_content))
            response_headers['Content-Type'] = content_type

        return response_headers

    @classmethod
    def determine_password_state(cls, smooth_streams_password):
        password_state = SmoothStreamsProxyPasswordState.ENCRYPTED

        try:
            base64_decoded_encrypted_fernet_token = base64.urlsafe_b64decode(smooth_streams_password)

            if base64_decoded_encrypted_fernet_token[0] == 0x80:
                length_of_base64_decoded_encrypted_fernet_token = len(base64_decoded_encrypted_fernet_token)

                if length_of_base64_decoded_encrypted_fernet_token < 73 or \
                        (length_of_base64_decoded_encrypted_fernet_token - 57) % 16 != 0:
                    password_state = SmoothStreamsProxyPasswordState.DECRYPTED
            else:
                password_state = SmoothStreamsProxyPasswordState.DECRYPTED
        except binascii.Error:
            password_state = SmoothStreamsProxyPasswordState.DECRYPTED

        return password_state

    @classmethod
    def determine_private_ip_address(cls):
        private_ip_address = None

        try:
            socket_object = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            socket_object.connect(("8.8.8.8", 80))

            private_ip_address = socket_object.getsockname()[0]

            socket_object.close()
        except (IndexError, OSError):
            logger.error('Failed to determine private IP address')

        return private_ip_address

    @classmethod
    def determine_public_ip_address(cls):
        public_ip_address = None

        try:
            response = requests.get('https://httpbin.org/ip')

            if response.status_code == requests.codes.OK:
                public_ip_address = response.json()['origin']
        except (json.JSONDecodeError, requests.exceptions.RequestException):
            logger.error('Failed to determine public IP address')

        return public_ip_address

    @classmethod
    def decrypt_password(cls, fernet_key, encrypted_password):
        fernet = Fernet(fernet_key)

        return fernet.decrypt(encrypted_password.encode())

    @classmethod
    def encrypt_password(cls, fernet_key, decrypted_password):
        fernet = Fernet(fernet_key)

        return fernet.encrypt(decrypted_password.encode()).decode()

    @classmethod
    def initialize_logging(cls, log_file_path):
        logging.addLevelName(TRACE, 'TRACE')
        logging.TRACE = TRACE
        logging.trace = trace
        logging.Logger.trace = trace

        formatter = SmoothStreamsProxyMultiLineFormatter(
            '%(asctime)s %(module)-20s %(funcName)-40s %(levelname)-8s %(message)s')

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        rotating_file_handler = logging.handlers.RotatingFileHandler('{0}'.format(log_file_path),
                                                                     maxBytes=1024 * 1024 * 10,
                                                                     backupCount=10)
        rotating_file_handler.setFormatter(formatter)

        logging.getLogger('smooth_streams_proxy').addHandler(console_handler)
        logging.getLogger('smooth_streams_proxy').addHandler(rotating_file_handler)

        cls.set_logging_level(logging.INFO)

    @classmethod
    def is_valid_hostname(cls, hostname):
        if len(hostname) > 255:
            return False

        if hostname and hostname[-1] == ".":
            hostname = hostname[:-1]

        return re.match('\A(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*'
                        '([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])\Z', hostname)

    @classmethod
    def make_http_request(cls, requests_http_method, url, params=None, data=None, json_=None, headers=None,
                          cookies=None, timeout=60):
        try:
            # noinspection PyUnresolvedReferences
            logger.trace('Request\n'
                         '[Method]\n'
                         '========\n{0}\n\n'
                         '[URL]\n'
                         '=====\n{1}\n'
                         '{2}{3}{4}{5}'.format(requests_http_method.__name__.capitalize(),
                                               url,
                                               '\n'
                                               '[Parameters]\n'
                                               '============\n{0}\n'.format('\n'.join(
                                                   ['{0:32} => {1!s}'.format(key, params[key])
                                                    for key in sorted(params)])) if params else '',
                                               '\n'
                                               '[Headers]\n'
                                               '=========\n{0}\n'.format(
                                                   '\n'.join(
                                                       ['{0:32} => {1!s}'.format(key,
                                                                                 pprint.pformat(headers[key], indent=2))
                                                        for key in sorted(headers)])) if headers else '',
                                               '\n'
                                               '[Cookies]\n'
                                               '=========\n{0}\n'.format(
                                                   '\n'.join(
                                                       ['{0:32} => {1!s}'.format(key,
                                                                                 pprint.pformat(cookies[key], indent=2))
                                                        for key in sorted(cookies)])) if cookies else '',
                                               '\n'
                                               '[JSON]\n'
                                               '======\n{0}\n'.format(
                                                   json.dumps(json_,
                                                              sort_keys=True,
                                                              indent=2)) if json_ else '').strip())

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
    def parse_command_line_arguments(cls):
        parser = ArgumentParser()

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
                            dest='recordings_directory_path',
                            help='path to the recordings folder',
                            metavar='recordings folder path')
        parser.add_argument('-s',
                            action='store',
                            default='smooth_streams_proxy_db',
                            dest='shelf_file_path',
                            help='path to the shelf file',
                            metavar='shelf file path')

        arguments = parser.parse_args()

        return (arguments.configuration_file_path,
                arguments.log_file_path,
                arguments.recordings_directory_path,
                arguments.shelf_file_path)

    @classmethod
    def set_logging_level(cls, log_level):
        logging.getLogger('smooth_streams_proxy').setLevel(log_level)

        for handler in logger.handlers:
            handler.setLevel(log_level)


def trace(self, msg, *args, **kwargs):
    if self.isEnabledFor(TRACE):
        self._log(TRACE, msg, args, **kwargs)
