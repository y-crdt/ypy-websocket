import asyncio
import tempfile
from pathlib import Path

import pytest

from ypy_websocket.ystore import SQLiteYStore, TempFileYStore


class MetadataCallback:
    def __init__(self):
        self.i = 0

    def __call__(self):
        future = asyncio.Future()
        future.set_result(bytes(self.i))
        self.i += 1
        return future


class MyTempFileYStore(TempFileYStore):
    prefix_dir = "test_temp_"


class MySQLiteYStore(SQLiteYStore):
    db_path = str(Path(tempfile.mkdtemp(prefix="test_sql_")) / "ystore.db")


@pytest.mark.asyncio
@pytest.mark.parametrize("YStore", (MyTempFileYStore, MySQLiteYStore))
async def test_file_ystore(YStore):
    store_name = "my_store"
    ystore = YStore(store_name, metadata_callback=MetadataCallback())
    data = [b"foo", b"bar", b"baz"]
    for d in data:
        await ystore.write(d)

    if YStore == MyTempFileYStore:
        assert (Path(MyTempFileYStore.base_dir) / store_name).exists()
    elif YStore == MySQLiteYStore:
        assert Path(MySQLiteYStore.db_path).exists()
    i = 0
    async for d, m in ystore.read():
        assert d == data[i]  # data
        assert m == bytes(i)  # metadata
        i += 1
