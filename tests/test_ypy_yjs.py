import asyncio

import pytest
import y_py as Y
from websockets import connect  # type: ignore

from ypy_websocket import WebsocketProvider, YDoc


class YTest:
    def __init__(self, ydoc: YDoc, timeout: float = 1.0):
        self.ydoc = ydoc
        self.timeout = timeout
        self.ytest = ydoc.get_map("_test")
        self.clock = -1.0

    def run_clock(self):
        self.clock = max(self.clock, 0.0)
        with self.ydoc.begin_transaction() as t:
            self.ytest.set(t, "clock", self.clock)

    async def clock_run(self):
        change = asyncio.Event()

        def callback(event):
            if "clock" in event.keys:
                clk = self.ytest["clock"]
                if clk > self.clock:
                    self.clock = clk + 1.0
                    change.set()

        subscription_id = self.ytest.observe(callback)
        await asyncio.wait_for(change.wait(), timeout=self.timeout)
        self.ytest.unobserve(subscription_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("yjs_client", "0", indirect=True)
async def test_ypy_yjs_0(yws_server, yjs_client):
    ydoc = YDoc()
    ytest = YTest(ydoc)
    websocket = await connect("ws://localhost:1234/my-roomname")
    WebsocketProvider(ydoc, websocket)
    ymap = ydoc.get_map("map")
    # set a value in "in"
    for v_in in range(10):
        with ydoc.begin_transaction() as t:
            ymap.set(t, "in", float(v_in))
        ytest.run_clock()
        await ytest.clock_run()
        v_out = ymap["out"]
        assert v_out == v_in + 1.0


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
        cells = [Y.YMap({"source": Y.YText("1 + 2"), "metadata": {"foo": "bar"}}) for _ in range(3)]
        with ydoc.begin_transaction() as t:
            ycells.push(t, cells)
            ystate.set(t, "state", {"dirty": False})
        with ydoc.begin_transaction() as t:
            ycells.delete(t, 0, len(cells))
            for key in ystate:
                ystate.delete(t, key)
