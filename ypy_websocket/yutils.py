from enum import IntEnum
from typing import List


class YMessageType(IntEnum):
    SYNC = 0
    SYNC_STEP1 = 0
    SYNC_STEP2 = 1
    SYNC_UPDATE = 2


def write_var_uint(num: int) -> List[int]:
    res = []
    while num > 127:
        res += [128 | (127 & num)]
        num >>= 7
    res += [num]
    return res


def create_message(data: List[int], msg_type: int) -> bytes:
    return bytes([YMessageType.SYNC, msg_type] + write_var_uint(len(data)) + data)


def create_sync_step1_message(data: List[int]) -> bytes:
    return create_message(data, YMessageType.SYNC_STEP1)


def create_sync_step2_message(data: List[int]) -> bytes:
    return create_message(data, YMessageType.SYNC_STEP2)


def create_update_message(data: List[int]) -> bytes:
    return create_message(data, YMessageType.SYNC_UPDATE)


def get_messages(message):
    length = len(message)
    if not length:
        return
    i0 = 0
    while True:
        msg_len = 0
        i = 0
        while True:
            byte = message[i0]
            msg_len += (byte & 127) << i
            i += 7
            i0 += 1
            length -= 1
            if byte < 128:
                break
        i1 = i0 + msg_len
        msg = message[i0:i1]
        length -= msg_len
        yield msg
        if length == 0:
            return
        if length < 0:
            raise RuntimeError("Y protocol error")
        i0 = i1


def get_message(message):
    return next(get_messages(message))
