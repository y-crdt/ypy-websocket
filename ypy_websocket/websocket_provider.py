import asyncio

import y_py as Y

from .yutils import (
    YMessageType,
    create_sync_step1_message,
    create_sync_step2_message,
    get_message,
)
from .ydoc import YDoc


class WebsocketProvider:

    _update_queue: asyncio.Queue
    _ydoc: YDoc

    def __init__(self, ydoc: YDoc, websocket):
        self._ydoc = ydoc
        self._websocket = websocket
        asyncio.create_task(self._run())

    async def _run(self):
        state = Y.encode_state_vector(self._ydoc)
        msg = create_sync_step1_message(state)
        await self._websocket.send(msg)
        send_task = asyncio.create_task(self._send())
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
        send_task.cancel()

    async def _send(self):
        while True:
            update = await self._ydoc._update_queue.get()
            await self._websocket.send(update)
