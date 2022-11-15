import asyncio
import logging
from functools import partial

import y_py as Y

from .yutils import process_message, put_updates, sync


class WebsocketProvider:

    _ydoc: Y.YDoc
    _update_queue: asyncio.Queue

    def __init__(self, ydoc: Y.YDoc, websocket, log=None):
        self._ydoc = ydoc
        self._websocket = websocket
        self.log = log or logging.getLogger(__name__)
        self._update_queue = asyncio.Queue()
        ydoc.observe_after_transaction(partial(put_updates, self._update_queue, ydoc))
        asyncio.create_task(self._run())

    async def _run(self):
        await sync(self._ydoc, self._websocket)
        send_task = asyncio.create_task(self._send())
        async for message in self._websocket:
            await process_message(message, self._ydoc, self._websocket, self.log)
        send_task.cancel()

    async def _send(self):
        while True:
            update = await self._update_queue.get()
            try:
                await self._websocket.send(update)
            except Exception:
                pass
