import asyncio
from types import TracebackType
from typing import Optional, Type

import y_py as Y

from .yutils import (
    YMessageType,
    create_sync_step1_message,
    create_sync_step2_message,
    create_update_message,
    get_message,
)


class YDoc(Y.YDoc):

    initialized: asyncio.Event
    synced: asyncio.Event
    _update_queue: asyncio.Queue

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.initialized = asyncio.Event()
        self.initialized.set()
        self.synced = asyncio.Event()
        self._update_queue = asyncio.Queue()

    _begin_transaction = Y.YDoc.begin_transaction

    def begin_transaction(self):
        return Transaction(self, self._update_queue)


class Transaction:

    ydoc: YDoc
    update_queue: asyncio.Queue

    def __init__(self, ydoc: YDoc, update_queue: asyncio.Queue):
        self.ydoc = ydoc
        self.update_queue = update_queue

    def __enter__(self):
        if self.ydoc.initialized.is_set():
            self.state = Y.encode_state_vector(self.ydoc)
        self.transaction = self.ydoc._begin_transaction()
        return self.transaction.__enter__()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ):
        res = self.transaction.__exit__(exc_type, exc_value, exc_tb)
        if self.ydoc.initialized.is_set():
            update = Y.encode_state_as_update(self.ydoc, self.state)
            message = create_update_message(update)
            self.update_queue.put_nowait(message)
        return res


async def process_message(message: bytes, ydoc: YDoc, websocket):
    await ydoc.initialized.wait()
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
            if message_type == YMessageType.SYNC_STEP2:
                ydoc.synced.set()
            return update


async def sync(ydoc: YDoc, websocket):
    await ydoc.initialized.wait()
    state = Y.encode_state_vector(ydoc)
    msg = create_sync_step1_message(state)
    await websocket.send(msg)
