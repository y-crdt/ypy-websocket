import asyncio
from types import TracebackType
from typing import Any, Dict, Optional, Type

import y_py as Y

from .yutils import (
    YMessageType,
    create_sync_step1_message,
    create_sync_step2_message,
    create_update_message,
    get_message,
)


class YDoc(Y.YDoc):

    _update_queue: Optional[asyncio.Queue]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._update_queue = None

    _begin_transaction = Y.YDoc.begin_transaction

    def begin_transaction(self):
        if self._update_queue is None:
            return self._begin_transaction()
        else:
            return Transaction(self, self._update_queue)


class WebsocketProvider:

    _update_queue: asyncio.Queue
    _ydoc: YDoc
    _room: str
    _ws_opts: Dict[str, Any]

    def __init__(self, ydoc: YDoc, websocket):
        self._websocket = websocket
        self._update_queue = asyncio.Queue()
        ydoc._update_queue = self._update_queue
        self._ydoc = ydoc
        asyncio.create_task(self._run())

    async def _run(self):
        state = Y.encode_state_vector(self._ydoc)
        msg = create_sync_step1_message(state)
        await self._websocket.send(msg)
        self._send_task = asyncio.create_task(self._send())
        asyncio.create_task(self._recv())

    async def _recv(self):
        async for message in self._websocket:
            if message[0] == YMessageType.SYNC:
                message_type = message[1]
                msg = message[2:]
                if message_type == YMessageType.SYNC_STEP1:
                    state = get_message(msg)
                    update = Y.encode_state_as_update(self._ydoc, state)
                    reply = create_sync_step2_message(update)
                    await self._websocket.send(reply)
                elif message_type in (
                    YMessageType.SYNC_STEP2,
                    YMessageType.SYNC_UPDATE,
                ):
                    update = get_message(msg)
                    Y.apply_update(self._ydoc, update)
        self._send_task.cancel()

    async def _send(self):
        while True:
            update = await self._update_queue.get()
            await self._websocket.send(update)


class Transaction:

    ydoc: YDoc
    update_queue: asyncio.Queue

    def __init__(self, ydoc: YDoc, update_queue: asyncio.Queue):
        self.ydoc = ydoc
        self.update_queue = update_queue

    def __enter__(self):
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
        update = Y.encode_state_as_update(self.ydoc, self.state)
        message = create_update_message(update)
        self.update_queue.put_nowait(message)
        return res
