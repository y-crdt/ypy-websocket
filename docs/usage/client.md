A client connects their `YDoc` through a [WebsocketProvider](../reference/WebSocket_provider.md).

Here is a code example using the [websockets](https://websockets.readthedocs.io) library:
```py
import asyncio
import y_py as Y
from websockets import connect
from ypy_websocket import WebsocketProvider

async def client():
    ydoc = Y.YDoc()
    async with (
        connect("ws://localhost:1234/my-roomname") as websocket,
        WebsocketProvider(ydoc, websocket) as provider,
    ):
        ymap = ydoc.get_map("map")
        
        # Wait until we've received the initial state from the server.
        await provider.synced.wait()
        print(ymap.to_json())

        # Changes to remote ydoc are applied to local ydoc.
        # Changes to local ydoc are sent over the WebSocket and
        # broadcast to all clients.
        with ydoc.begin_transaction() as t:
            ymap.set(t, "key", "value")

        await asyncio.Future()  # run forever

asyncio.run(client())
```
