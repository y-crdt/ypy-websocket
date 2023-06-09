from __future__ import annotations

import logging
from contextlib import AsyncExitStack

from anyio import TASK_STATUS_IGNORED, Event, create_task_group
from anyio.abc import TaskGroup, TaskStatus

from .yroom import YRoom
from .yutils import YMessageType, process_sync_message, sync


class WebsocketServer:

    auto_clean_rooms: bool
    rooms: dict[str, YRoom]
    _started: Event | None
    _task_group: TaskGroup | None

    def __init__(self, rooms_ready: bool = True, auto_clean_rooms: bool = True, log=None):
        self.rooms_ready = rooms_ready
        self.auto_clean_rooms = auto_clean_rooms
        self.log = log or logging.getLogger(__name__)
        self.rooms = {}
        self._started = None
        self._task_group = None

    @property
    def started(self):
        if self._started is None:
            self._started = Event()
        return self._started

    def get_room(self, path: str) -> YRoom:
        if path not in self.rooms.keys():
            self.rooms[path] = YRoom(ready=self.rooms_ready, log=self.log)
        return self.rooms[path]

    def get_room_name(self, room):
        return list(self.rooms.keys())[list(self.rooms.values()).index(room)]

    def rename_room(
        self, to_name: str, *, from_name: str | None = None, from_room: YRoom | None = None
    ):
        if from_name is not None and from_room is not None:
            raise RuntimeError("Cannot pass from_name and from_room")
        if from_name is None:
            from_name = self.get_room_name(from_room)
        self.rooms[to_name] = self.rooms.pop(from_name)

    def delete_room(self, *, name: str | None = None, room: YRoom | None = None):
        if name is not None and room is not None:
            raise RuntimeError("Cannot pass name and room")
        if name is None:
            name = self.get_room_name(room)
        room = self.rooms[name]
        room.stop()
        del self.rooms[name]

    async def serve(self, websocket):
        if self._task_group is None:
            raise RuntimeError(
                "The WebsocketServer is not running: use `async with websocket_server:` or `await websocket_server.start()`"
            )

        await self._task_group.start(self._serve, websocket)

    async def _serve(self, websocket, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        async with create_task_group() as tg:
            room = self.get_room(websocket.path)
            if not room.started.is_set():
                tg.start_soon(room.start)
                await room.started.wait()
            room.clients.append(websocket)
            await sync(room.ydoc, websocket, self.log)
            try:
                async for message in websocket:
                    # filter messages (e.g. awareness)
                    skip = False
                    if room.on_message:
                        skip = await room.on_message(message)
                    if skip:
                        continue
                    message_type = message[0]
                    if message_type == YMessageType.SYNC:
                        # update our internal state in the background
                        # changes to the internal state are then forwarded to all clients
                        # and stored in the YStore (if any)
                        tg.start_soon(
                            process_sync_message, message[1:], room.ydoc, websocket, self.log
                        )
                    elif message_type == YMessageType.AWARENESS:
                        # forward awareness messages from this client to all clients,
                        # including itself, because it's used to keep the connection alive
                        self.log.debug(
                            "Received %s message from endpoint: %s",
                            YMessageType.AWARENESS.name,
                            websocket.path,
                        )
                        for client in room.clients:
                            self.log.debug(
                                "Sending Y awareness from client with endpoint %s to client with endpoint: %s",
                                websocket.path,
                                client.path,
                            )
                            tg.start_soon(client.send, message)
            except Exception:
                pass

            # remove this client
            room.clients = [c for c in room.clients if c != websocket]
            if self.auto_clean_rooms and not room.clients:
                self.delete_room(room=room)
            tg.cancel_scope.cancel()
            task_status.started()

    async def __aenter__(self):
        if self._task_group is not None:
            raise RuntimeError("WebsocketServer already running")

        async with AsyncExitStack() as exit_stack:
            tg = create_task_group()
            self._task_group = await exit_stack.enter_async_context(tg)
            self._exit_stack = exit_stack.pop_all()
            self.started.set()

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        self._task_group.cancel_scope.cancel()
        self._task_group = None
        return await self._exit_stack.__aexit__(exc_type, exc_value, exc_tb)

    async def start(self):
        if self._task_group is not None:
            raise RuntimeError("WebsocketServer already running")

        # create the task group and wait forever
        async with create_task_group() as self._task_group:
            self._task_group.start_soon(Event().wait)
            self.started.set()

    def stop(self):
        self._task_group.cancel_scope.cancel()
        self._task_group = None
