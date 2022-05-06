import asyncio

from .ydoc import YDoc, process_message, sync


class WebsocketProvider:

    _ydoc: YDoc

    def __init__(self, ydoc: YDoc, websocket):
        self._ydoc = ydoc
        self._websocket = websocket
        asyncio.create_task(self._run())

    async def _run(self):
        await sync(self._ydoc, self._websocket)
        send_task = asyncio.create_task(self._send())
        async for message in self._websocket:
            await process_message(message, self._ydoc, self._websocket)
        send_task.cancel()

    async def _send(self):
        while True:
            update = await self._ydoc._update_queue.get()
            try:
                await self._websocket.send(update)
            except Exception:
                pass
