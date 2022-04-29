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
    # set a value in "in"
    for v_in in ["0", "1"]:
        with ydoc.begin_transaction() as t:
            ymap.set(t, "in", v_in)
        # wait for the JS client to return a value in "out"
        change = asyncio.Event()

        def callback(event):
            if "out" in event.keys:
                change.set()

        ymap.observe(callback)
        await asyncio.wait_for(change.wait(), timeout=1)
        v_out = ymap["out"]
        assert v_out == v_in + "1"


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


@pytest.mark.asyncio
@pytest.mark.parametrize("yjs_client", "2", indirect=True)
async def test_ypy_yjs_2(yws_server, yjs_client):
    ydoc = YDoc()
    websocket = await connect("ws://localhost:1234/my-roomname")
    WebsocketProvider(ydoc, websocket)
    ytext = ydoc.get_text("text")
    with ydoc.begin_transaction() as t:
        ytext.push(t, "a")
    with ydoc.begin_transaction() as t:
        ytext.delete(t, 0, len(ytext))
    with ydoc.begin_transaction() as t:
        ytext.push(t, "")
    await asyncio.sleep(0.3)
