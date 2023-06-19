import subprocess

import pytest
import y_py as Y
from websockets import serve  # type: ignore

from ypy_websocket import WebsocketServer


class TestYDoc:
    def __init__(self):
        self.ydoc = Y.YDoc()
        self.array = self.ydoc.get_array("array")
        self.state = None
        self.value = 0

    def update(self):
        with self.ydoc.begin_transaction() as txn:
            self.array.append(txn, self.value)
        self.value += 1
        update = Y.encode_state_as_update(self.ydoc, self.state)
        self.state = Y.encode_state_vector(self.ydoc)
        return update


@pytest.fixture
async def yws_server(request):
    try:
        kwargs = request.param
    except Exception:
        kwargs = {}
    websocket_server = WebsocketServer(**kwargs)
    try:
        async with websocket_server, serve(websocket_server.serve, "127.0.0.1", 1234):
            yield websocket_server
    except Exception:
        pass


@pytest.fixture
def yjs_client(request):
    client_id = request.param
    p = subprocess.Popen(["node", f"tests/yjs_client_{client_id}.js"])
    yield p
    p.kill()


@pytest.fixture
def test_ydoc():
    return TestYDoc()


@pytest.fixture
def anyio_backend():
    return "asyncio"
