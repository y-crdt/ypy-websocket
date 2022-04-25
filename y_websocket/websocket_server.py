import asyncio
from typing import Dict, List, Optional

import y_py as Y

from .yutils import (
    YMessageType,
    create_sync_step1_message,
    create_sync_step2_message,
    get_message,
)
from .ydoc import YDoc


class YRoom:

    clients: List
    ydoc: Optional[YDoc]

    def __init__(self, has_internal_ydoc: bool = False):
        self.clients = []
        if has_internal_ydoc:
            self.ydoc = YDoc()
        else:
            self.ydoc = None


class WebsocketServer:

    rooms: Dict[str, YRoom]

    def __init__(self, has_internal_ydoc: bool = False, auto_clean_rooms: bool = True):
        self.has_internal_ydoc = has_internal_ydoc
        self.auto_clean_rooms = auto_clean_rooms
        self.rooms = {}

    def get_room(self, path: str) -> YRoom:
        room = self.rooms.get(path, YRoom(self.has_internal_ydoc))
        self.rooms[path] = room
        return room

    def get_room_name(self, room):
        return list(self.rooms.keys())[list(self.rooms.values()).index(room)]

    def on_message(self, message):
        pass

    def rename_room(
        self,
        to_name: str,
        *,
        from_name: Optional[str] = None,
        from_room: Optional[YRoom] = None
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
        del self.rooms[name]

    async def serve(self, websocket):
        room = self.get_room(websocket.path)
        room.clients.append(websocket)
        if room.ydoc is not None:
            state = Y.encode_state_vector(room.ydoc)
            msg = create_sync_step1_message(state)
            await websocket.send(msg)
            send_task = asyncio.create_task(self._send(room.ydoc, room.clients))
        else:
            send_task = None
        async for message in websocket:
            # forward messages to every other client
            for client in [c for c in room.clients if c != websocket]:
                await client.send(message)
            if room.ydoc is not None:
                if message[0] == YMessageType.SYNC:
                    message_type = message[1]
                    msg = message[2:]
                    if message_type == YMessageType.SYNC_STEP1:
                        state = get_message(msg)
                        update = Y.encode_state_as_update(room.ydoc, state)
                        reply = create_sync_step2_message(update)
                        await websocket.send(reply)
                    elif message_type in (
                        YMessageType.SYNC_STEP2,
                        YMessageType.SYNC_UPDATE,
                    ):
                        update = get_message(msg)
                        Y.apply_update(room.ydoc, update)
        if send_task is not None:
            send_task.cancel()
        # remove this client
        room.clients = [c for c in room.clients if c != websocket]
        if self.auto_clean_rooms and not room.clients:
            self.delete_room(room=room)

    async def _send(self, ydoc, clients):
        while True:
            update = await ydoc._update_queue.get()
            # broadcast update to all clients
            for client in clients:
                await client.send(update)
