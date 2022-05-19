import asyncio
from typing import Callable, Dict, List, Optional

import aiofiles  # type: ignore
import y_py as Y

from .ydoc import YDoc, process_message, sync
from .yutils import get_messages, write_var_uint


class YRoom:

    clients: List
    ydoc: YDoc
    updates_file_path: Optional[str]
    _on_message: Optional[Callable]

    def __init__(self, ready: bool = True, updates_file_path: Optional[str] = None):
        self._ready = ready
        self.clients = []
        self.ydoc = YDoc()
        if not ready:
            self.ydoc.initialized.clear()
        self._on_message = None
        self._broadcast_task = asyncio.create_task(self._broadcast_updates())
        self.updates_file_path = updates_file_path
        if updates_file_path:
            open(updates_file_path, "wb").close()

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

    async def encode_state_as_update_to_file(self, path: Optional[str] = None):
        path = path or self.updates_file_path
        update = Y.encode_state_as_update(self.ydoc)  # type: ignore
        async with aiofiles.open(path, "ab") as f:
            var_len = write_var_uint(len(update))
            await f.write(bytes(var_len + update))

    async def apply_updates_from_file(self, path: Optional[str] = None):
        path = path or self.updates_file_path
        async with aiofiles.open(path, "rb") as f:
            updates = await f.read()
        for _, update in get_messages(updates):
            Y.apply_update(self.ydoc, update)


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
            res = await process_message(message, room.ydoc, websocket)
            if room.updates_file_path and res is not None:
                var_len, update = res
                async with aiofiles.open(room.updates_file_path, "ab") as f:
                    await f.write(var_len + update)
        # remove this client
        room.clients = [c for c in room.clients if c != websocket]
        if self.auto_clean_rooms and not room.clients:
            self.delete_room(room=room)
