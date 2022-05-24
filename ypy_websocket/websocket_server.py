import asyncio
from typing import Callable, Dict, List, Optional

from .ydoc import YDoc, process_message, sync
from .ystore import BaseYStore


class YRoom:

    clients: List
    ydoc: YDoc
    ystore: Optional[BaseYStore]
    _on_message: Optional[Callable]

    def __init__(self, ready: bool = True, ystore: Optional[BaseYStore] = None):
        self._ready = ready
        self.ystore = ystore
        self.clients = []
        self.ydoc = YDoc()
        if not ready:
            self.ydoc.initialized.clear()
        self._on_message = None
        self._broadcast_task = asyncio.create_task(self._broadcast_updates())

    @property
    def ready(self) -> bool:
        return self._ready

    @ready.setter
    def ready(self, value: bool) -> None:
        if value:
            self._ready = True
            self.ydoc.initialized.set()

    @property
    def on_message(self) -> Optional[Callable]:
        return self._on_message

    @on_message.setter
    def on_message(self, value: Optional[Callable]):
        self._on_message = value

    async def _broadcast_updates(self):
        try:
            await self.ydoc.synced.wait()
            while True:
                update = await self.ydoc._update_queue.get()
                # broadcast internal ydoc's update to all clients
                for client in self.clients:
                    try:
                        await client.send(update)
                    except Exception:
                        pass
        except Exception:
            pass

    def _clean(self):
        self._broadcast_task.cancel()


class WebsocketServer:

    auto_clean_rooms: bool
    rooms: Dict[str, YRoom]

    def __init__(self, rooms_ready: bool = True, auto_clean_rooms: bool = True):
        self.rooms_ready = rooms_ready
        self.auto_clean_rooms = auto_clean_rooms
        self.rooms = {}

    def get_room(self, path: str) -> YRoom:
        if path not in self.rooms.keys():
            self.rooms[path] = YRoom(ready=self.rooms_ready)
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
            if room.on_message:
                await room.on_message(message)
            # forward messages to every other client
            for client in [c for c in room.clients if c != websocket]:
                await client.send(message)
            # update our internal state
            update = await process_message(message, room.ydoc, websocket)
            if room.ystore and update:
                await room.ystore.write(update)
        # remove this client
        room.clients = [c for c in room.clients if c != websocket]
        if self.auto_clean_rooms and not room.clients:
            self.delete_room(room=room)
