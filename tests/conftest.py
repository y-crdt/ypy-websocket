import subprocess

import pytest
from websockets import serve  # type: ignore
from y_websocket import WebsocketServer


@pytest.fixture
async def echo_server():
    websocket_server = WebsocketServer()
    async with serve(websocket_server.serve, "localhost", 1234):
        yield


@pytest.fixture
def yjs_client():
    p = subprocess.Popen(["node", "tests/yjs_client.js"])
    yield p
    p.kill()
