import asyncio

import pytest
from y_websocket import YDoc, WebsocketProvider


@pytest.mark.asyncio
async def test_ypy_yjs(echo_server, yjs_client):
    ydoc = YDoc()
    WebsocketProvider("ws://localhost:1234", "my-roomname", ydoc)
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
