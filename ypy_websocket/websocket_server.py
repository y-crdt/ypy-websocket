import asyncio
import logging
from typing import Callable, Dict, List, Optional

from .awareness import Awareness
from .ydoc import YDoc
from .ystore import BaseYStore
from .yutils import sync, update


class YRoom:

    clients: List
    ydoc: YDoc
    ystore: Optional[BaseYStore]
    _on_message: Optional[Callable]
    _update_queue: asyncio.Queue
    _ready: bool

    def __init__(self, ready: bool = True, ystore: Optional[BaseYStore] = None, log=None):
        self._update_queue = asyncio.Queue()
        self.ydoc = YDoc()
        self.ydoc.init(self._update_queue)  # FIXME: overriding Y.YDoc.__init__ doesn't seem to work
        self.awareness = Awareness(self.ydoc)
        self._ready = False
        self.ready = ready
        self.ystore = ystore
        self.log = log or logging.getLogger(__name__)
        self.clients = []
        self._on_message = None
        self._broadcast_task = asyncio.create_task(self._broadcast_updates())

    @property
    def ready(self) -> bool:
        return self._ready

    @ready.setter
    def ready(self, value: bool) -> None:
        self._ready = value
        if value:
            self.ydoc._ready = True

    @property
    def on_message(self) -> Optional[Callable]:
        return self._on_message

    @on_message.setter
    def on_message(self, value: Optional[Callable]):
        self._on_message = value

    async def _broadcast_updates(self):
        while True:
            update = await self._update_queue.get()
            # broadcast internal ydoc's update made from the backend to all clients
            for client in self.clients:
                self.log.debug(
                    "Sending Y update from backend to client with endpoint: %s", client.path
                )
                asyncio.create_task(client.send(update))

    def _clean(self):
        self._broadcast_task.cancel()


class WebsocketServer:

    auto_clean_rooms: bool
    rooms: Dict[str, YRoom]

    def __init__(self, rooms_ready: bool = True, auto_clean_rooms: bool = True, log=None):
        self.rooms_ready = rooms_ready
        self.auto_clean_rooms = auto_clean_rooms
        self.log = log or logging.getLogger(__name__)
        self.rooms = {}

    def get_room(self, path: str) -> YRoom:
        if path not in self.rooms.keys():
            self.rooms[path] = YRoom(ready=self.rooms_ready, log=self.log)
        return self.rooms[path]

    def get_room_name(self, room):
        return list(self.rooms.keys())[list(self.rooms.values()).index(room)]

    def rename_room(
        self, to_name: str, *, from_name: Optional[str] = None, from_room: Optional[YRoom] = None
    ):
        if from_name is not None and from_room is not None:
            raise RuntimeError("Cannot pass from_name and from_room")
        if from_name is None:
            from_name = self.get_room_name(from_room)
        self.rooms[to_name] = self.rooms.pop(from_name)

    def delete_room(self, *, name: Optional[str] = None, room: Optional[YRoom] = None):
        if name is not None and room is not None:
            raise RuntimeError("Cannot pass name and room")
        if name is None:
            name = self.get_room_name(room)
        room = self.rooms[name]
        room._clean()
        del self.rooms[name]

    async def serve(self, websocket):
        room = self.get_room(websocket.path)
        room.clients.append(websocket)
        await sync(room.ydoc, websocket)
        async for message in websocket:
            # filter messages (e.g. awareness)
            skip = False
            if room.on_message:
                skip = await room.on_message(message)
            if skip:
                continue
            # update our internal state and the YStore (if any)
            asyncio.create_task(update(message, room, websocket, self.log))
            # forward messages from this client to every other client in the background
            for client in [c for c in room.clients if c != websocket]:
                self.log.debug("Sending Y update from client with endpoint: %s", websocket.path)
                self.log.debug("... to client with endpoint: %s", client.path)
                asyncio.create_task(client.send(message))
        # remove this client
        room.clients = [c for c in room.clients if c != websocket]
        if self.auto_clean_rooms and not room.clients:
            self.delete_room(room=room)
