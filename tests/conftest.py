import subprocess
import tempfile
from pathlib import Path

import pytest
from websockets import serve  # type: ignore

from ypy_websocket import WebsocketServer


@pytest.fixture
async def yws_server(request):
    try:
        kwargs = request.param
    except Exception:
        kwargs = {}
    websocket_server = WebsocketServer(**kwargs)
    async with serve(websocket_server.serve, "localhost", 1234):
        yield websocket_server


@pytest.fixture
def yjs_client(request):
    client_id = request.param
    p = subprocess.Popen(["node", f"tests/yjs_client_{client_id}.js"])
    yield p
    p.kill()

@pytest.fixture
def yjs_sqlite_db_path():
    return str(Path(tempfile.mkdtemp(prefix="test_sql_")) / "ystore.db")
