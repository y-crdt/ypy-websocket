[![Build Status](https://github.com/davidbrochart/y-websocket/workflows/CI/badge.svg)](https://github.com/davidbrochart/y-websocket/actions)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


# y-websocket

y-websocket is an async WebSocket connector for Ypy.

## Usage

Here is a code example:

```py
import asyncio
from websockets import connect
from y_websocket import YDoc, WebsocketProvider

async def main():
    ydoc = YDoc()
    websocket = await connect("ws://localhost:1234")
    WebsocketProvider(ydoc, websocket)
    ymap = ydoc.get_map("map")
    with ydoc.begin_transaction() as t:
        ymap.set(t, "key", "value")

asyncio.run(main())
```

Note that `YDoc` has to be imported from `y_websocket` instead of `y_py`. This will change in the
future, when `y_py` has the necessary event handlers. `y_websocket.YDoc` is a subclass of
`y_py.YDoc`.
