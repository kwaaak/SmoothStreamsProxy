from enum import Enum


class SmoothStreamsProxyPasswordState(Enum):
    DECRYPTED = 0
    ENCRYPTED = 1


class SmoothStreamsProxyRecordingStatus(Enum):
    ACTIVE = 'active'
    PERSISTED = 'persisted'
    SCHEDULED = 'scheduled'
