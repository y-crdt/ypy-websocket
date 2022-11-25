[![Build Status](https://github.com/y-crdt/ypy-websocket/workflows/CI/badge.svg)](https://github.com/y-crdt/ypy-websocket/actions)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


# ypy-websocket

ypy-websocket is an async WebSocket connector for Ypy.

## Usage

### Client

Here is a code example:

```py
import asyncio
import y_py as Y
from websockets import connect
from ypy_websocket import WebsocketProvider

async def client():
    ydoc = Y.YDoc()
    async with connect("ws://localhost:1234/my-roomname") as websocket:
        WebsocketProvider(ydoc, websocket)
        ymap = ydoc.get_map("map")
        with ydoc.begin_transaction() as t:
            ymap.set(t, "key", "value")

asyncio.run(client())
```

### Server

Here is a code example:

```py
import asyncio
from websockets import serve
from ypy_websocket import WebsocketServer

async def server():
    websocket_server = WebsocketServer()
    async with serve(websocket_server.serve, "localhost", 1234):
        await asyncio.Future()  # run forever

asyncio.run(server())
```

### WebSocket API

The WebSocket object passed to `WebsocketProvider` and `WebsocketServer.serve` must respect the
following API:

```py
class WebSocket:

    @property
    def path(self) -> str:
        # can be e.g. the URL path
        # or a room identifier
        return "my-roomname"

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        # async iterator for receiving messages
        # until the connection is closed
        try:
            message = await self.recv()
        except:
            raise StopAsyncIteration()
        return message

    async def send(self, message: bytes):
        # send message
        pass

    async def recv(self) -> bytes:
        # receive message
        return b""
```
