from typing import Dict, List


class YRoom:

    clients: List

    def __init__(self):
        self.clients = []


class WebsocketServer:

    rooms: Dict[str, YRoom]

    def __init__(self):
        self.rooms = {}

    async def echo(self, websocket):
        room_id = websocket.path
        room = self.rooms.get(room_id, YRoom())
        self.rooms[room_id] = room
        room.clients.append(websocket)
        async for message in websocket:
            # forward messages to everybody else
            for client in [c for c in room.clients if c != websocket]:
                await client.send(message)
