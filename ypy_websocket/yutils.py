import asyncio
from enum import IntEnum

import y_py as Y


class YMessageType(IntEnum):
    SYNC = 0
    SYNC_STEP1 = 0
    SYNC_STEP2 = 1
    SYNC_UPDATE = 2


def write_var_uint(num: int) -> bytes:
    res = []
    while num > 127:
        res.append(128 | (127 & num))
        num >>= 7
    res.append(num)
    return bytes(res)


def create_message(data: bytes, msg_type: int) -> bytes:
    return bytes([YMessageType.SYNC, msg_type]) + write_var_uint(len(data)) + data


def create_sync_step1_message(data: bytes) -> bytes:
    return create_message(data, YMessageType.SYNC_STEP1)


def create_sync_step2_message(data: bytes) -> bytes:
    return create_message(data, YMessageType.SYNC_STEP2)


def create_update_message(data: bytes) -> bytes:
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


def put_updates(update_queue: asyncio.Queue, ydoc: Y.YDoc, event: Y.AfterTransactionEvent) -> None:
    message = create_update_message(event.get_update())
    update_queue.put_nowait(message)


async def process_message(message: bytes, ydoc: Y.YDoc, websocket):
    if message[0] == YMessageType.SYNC:
        message_type = message[1]
        msg = message[2:]
        if message_type == YMessageType.SYNC_STEP1:
            state = get_message(msg)
            update = Y.encode_state_as_update(ydoc, state)
            reply = create_sync_step2_message(update)
            await websocket.send(reply)
        elif message_type in (
            YMessageType.SYNC_STEP2,
            YMessageType.SYNC_UPDATE,
        ):
            update = get_message(msg)
            Y.apply_update(ydoc, update)
            return update


async def sync(ydoc: Y.YDoc, websocket):
    state = Y.encode_state_vector(ydoc)
    msg = create_sync_step1_message(state)
    await websocket.send(msg)
