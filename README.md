[![Build Status](https://github.com/davidbrochart/ypy-websocket/workflows/Tests/badge.svg)](https://github.com/davidbrochart/ypy-websocket/actions)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


# ypy-websocket

ypy-websocket is an async WebSocket connector for Ypy.

## Usage

### Client

Here is a code example:

```py
import asyncio
from websockets import connect
from ypy_websocket import YDoc, WebsocketProvider

async def client():
    ydoc = YDoc()
    websocket = await connect("ws://localhost:1234/my-roomname")
    WebsocketProvider(ydoc, websocket)
    ymap = ydoc.get_map("map")
    with ydoc.begin_transaction() as t:
        ymap.set(t, "key", "value")

asyncio.run(client())
```

Note that `YDoc` has to be imported from `ypy_websocket` instead of `y_py`. This will change in the
future, when `y_py` has the necessary event handlers. `ypy_websocket.YDoc` is a subclass of
`y_py.YDoc`.

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