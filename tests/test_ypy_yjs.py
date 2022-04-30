import asyncio

import pytest
from websockets import connect  # type: ignore
import y_py as Y
from ypy_websocket import YDoc, WebsocketProvider


@pytest.mark.asyncio
@pytest.mark.parametrize("yjs_client", "0", indirect=True)
async def test_ypy_yjs_0(yws_server, yjs_client):
    ydoc = YDoc()
    websocket = await connect("ws://localhost:1234/my-roomname")
    WebsocketProvider(ydoc, websocket)
    ymap = ydoc.get_map("map")
    with ydoc.begin_transaction() as t:
        ymap.set(t, "clock", 0)
        ytext = Y.YText("")
        ymap.set(t, "text", ytext)
    # wait for a change on the JS side
    change = asyncio.Event()

    def callback(event):
        if "clock" in event.keys:
            change.set()

    ymap.observe(callback)
    await asyncio.wait_for(change.wait(), timeout=1)
    assert str(ymap["text"]) == "i"


@pytest.mark.skip(reason="FIXME: hangs")
@pytest.mark.asyncio
@pytest.mark.parametrize("yws_server", [{"has_internal_ydoc": True}], indirect=True)
@pytest.mark.parametrize("yjs_client", "1", indirect=True)
async def test_ypy_yjs_1(yws_server, yjs_client):
    # wait for the JS client to connect
    while True:
        await asyncio.sleep(0.1)
        if "my-roomname" in yws_server.rooms:
            break
    ydoc = yws_server.rooms["my-roomname"].ydoc
    ycells = ydoc.get_array("cells")
    ystate = ydoc.get_map("state")
    for _ in range(3):
        cells = [
            Y.YMap({"source": Y.YText("1 + 2"), "metadata": {"foo": "bar"}})
            for _ in range(3)
        ]
        with ydoc.begin_transaction() as t:
            ycells.push(t, cells)
            ystate.set(t, "state", {"dirty": False})
        with ydoc.begin_transaction() as t:
            ycells.delete(t, 0, len(cells))
            for key in ystate:
                ystate.delete(t, key)
