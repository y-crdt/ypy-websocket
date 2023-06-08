import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import aiosqlite
import pytest

from ypy_websocket.ystore import SQLiteYStore, TempFileYStore


class MetadataCallback:
    def __init__(self):
        self.i = 0

    async def __call__(self):
        res = str(self.i).encode()
        self.i += 1
        return res


class MyTempFileYStore(TempFileYStore):
    prefix_dir = "test_temp_"


MY_SQLITE_YSTORE_DB_PATH = str(Path(tempfile.mkdtemp(prefix="test_sql_")) / "ystore.db")


class MySQLiteYStore(SQLiteYStore):
    db_path = MY_SQLITE_YSTORE_DB_PATH
    document_ttl = 1000

    def __init__(self, *args, delete_db=False, **kwargs):
        if delete_db:
            os.remove(self.db_path)
        super().__init__(*args, **kwargs)


@pytest.mark.anyio
@pytest.mark.parametrize("YStore", (MyTempFileYStore, MySQLiteYStore))
async def test_ystore(YStore):
    store_name = "my_store"
    ystore = YStore(store_name, metadata_callback=MetadataCallback())
    await ystore.start()
    data = [b"foo", b"bar", b"baz"]
    for d in data:
        await ystore.write(d)

    if YStore == MyTempFileYStore:
        assert (Path(MyTempFileYStore.base_dir) / store_name).exists()
    elif YStore == MySQLiteYStore:
        assert Path(MySQLiteYStore.db_path).exists()
    i = 0
    async for d, m, t in ystore.read():
        assert d == data[i]  # data
        assert m == str(i).encode()  # metadata
        i += 1

    assert i == len(data)


@pytest.mark.anyio
async def test_document_ttl_sqlite_ystore(test_ydoc):
    store_name = "my_store"
    ystore = MySQLiteYStore(store_name, delete_db=True)
    await ystore.start()
    now = time.time()

    for i in range(3):
        # assert that adding a record before document TTL doesn't delete document history
        with patch("time.time") as mock_time:
            mock_time.return_value = now
            await ystore.write(test_ydoc.update())
            async with aiosqlite.connect(ystore.db_path) as db:
                assert (await (await db.execute("SELECT count(*) FROM yupdates")).fetchone())[
                    0
                ] == i + 1

    # assert that adding a record after document TTL deletes previous document history
    with patch("time.time") as mock_time:
        mock_time.return_value = now + ystore.document_ttl + 1
        await ystore.write(test_ydoc.update())
        async with aiosqlite.connect(ystore.db_path) as db:
            # two updates in DB: one squashed update and the new update
            assert (await (await db.execute("SELECT count(*) FROM yupdates")).fetchone())[0] == 2


@pytest.mark.anyio
@pytest.mark.parametrize("YStore", (MyTempFileYStore, MySQLiteYStore))
async def test_version(YStore, caplog):
    store_name = "my_store"
    prev_version = YStore.version
    YStore.version = -1
    ystore = YStore(store_name)
    await ystore.start()
    await ystore.write(b"foo")
    YStore.version = prev_version
    assert "YStore version mismatch" in caplog.text
