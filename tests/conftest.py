import subprocess

import pytest
from websockets import serve  # type: ignore


CLIENTS = []


async def echo(websocket):
    CLIENTS.append(websocket)
    async for message in websocket:
        for client in [c for c in CLIENTS if c != websocket]:
            await client.send(message)


@pytest.fixture
async def echo_server():
    async with serve(echo, "localhost", 1234):
        yield


@pytest.fixture
def yjs_client():
    p = subprocess.Popen(["node", "tests/yjs_client.js"])
    yield p
    p.kill()
