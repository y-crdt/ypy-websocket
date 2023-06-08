import logging
from contextlib import AsyncExitStack
from functools import partial

import y_py as Y
from anyio import create_memory_object_stream, create_task_group
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from .yutils import (
    YMessageType,
    create_update_message,
    process_sync_message,
    put_updates,
    sync,
)


class WebsocketProvider:

    _ydoc: Y.YDoc
    _update_send_stream: MemoryObjectSendStream
    _update_receive_stream: MemoryObjectReceiveStream

    def __init__(self, ydoc: Y.YDoc, websocket, log=None):
        self._ydoc = ydoc
        self._websocket = websocket
        self.log = log or logging.getLogger(__name__)
        self._update_send_stream, self._update_receive_stream = create_memory_object_stream(
            max_buffer_size=65536
        )
        ydoc.observe_after_transaction(partial(put_updates, self._update_send_stream))

    async def __aenter__(self):
        async with AsyncExitStack() as exit_stack:
            tg = create_task_group()
            self._task_group = await exit_stack.enter_async_context(tg)
            self._exit_stack = exit_stack.pop_all()
            tg.start_soon(self._run)

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        self._task_group.cancel_scope.cancel()
        return await self._exit_stack.__aexit__(exc_type, exc_value, exc_tb)

    async def _run(self):
        await sync(self._ydoc, self._websocket, self.log)
        self._task_group.start_soon(self._send)
        async for message in self._websocket:
            if message[0] == YMessageType.SYNC:
                await process_sync_message(message[1:], self._ydoc, self._websocket, self.log)

    async def _send(self):
        async with self._update_receive_stream:
            async for update in self._update_receive_stream:
                message = create_update_message(update)
                try:
                    await self._websocket.send(message)
                except Exception:
                    pass
