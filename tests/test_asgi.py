import asyncio

import pytest
import uvicorn
import y_py as Y
from websockets import connect  # type: ignore

from ypy_websocket import ASGIServer, WebsocketProvider, WebsocketServer

websocket_server = WebsocketServer(auto_clean_rooms=False)
app = ASGIServer(websocket_server)


@pytest.mark.asyncio
async def test_asgi(unused_tcp_port):
    # server
    config = uvicorn.Config("test_asgi:app", port=unused_tcp_port, log_level="info")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0)

    # clients
    # client 1
    ydoc1 = Y.YDoc()
    ymap1 = ydoc1.get_map("map")
    with ydoc1.begin_transaction() as t:
        ymap1.set(t, "key", "value")
    async with connect(f"ws://localhost:{unused_tcp_port}/my-roomname") as websocket1:
        WebsocketProvider(ydoc1, websocket1)
        await asyncio.sleep(0.1)

    # client 2
    ydoc2 = Y.YDoc()
    async with connect(f"ws://localhost:{unused_tcp_port}/my-roomname") as websocket2:
        WebsocketProvider(ydoc2, websocket2)
        await asyncio.sleep(0.1)

    ymap2 = ydoc2.get_map("map")
    assert ymap2.to_json() == '{"key":"value"}'

    server_task.cancel()
