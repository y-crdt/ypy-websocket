import asyncio
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

    def __call__(self):
        future = asyncio.Future()
        future.set_result(str(self.i).encode())
        self.i += 1
        return future


class MyTempFileYStore(TempFileYStore):
    prefix_dir = "test_temp_"


MY_SQLITE_YSTORE_DB_PATH = str(Path(tempfile.mkdtemp(prefix="test_sql_")) / "ystore.db")


class MySQLiteYStore(SQLiteYStore):
    db_path = MY_SQLITE_YSTORE_DB_PATH
    document_ttl = 1

    def __init__(self, *args, delete_db=False, **kwargs):
        if delete_db:
            os.remove(self.db_path)
        super().__init__(*args, **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("YStore", (MyTempFileYStore, MySQLiteYStore))
async def test_ystore(YStore):
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
    async for d, m, t in ystore.read():
        assert d == data[i]  # data
        assert m == str(i).encode()  # metadata
        i += 1

    assert i == len(data)


async def count_yupdates(db):
    """Returns number of yupdates in a SQLite DB given a connection."""
    return (await (await db.execute("SELECT count(*) FROM yupdates")).fetchone())[0]


@pytest.mark.asyncio
async def test_document_ttl_sqlite_ystore(test_ydoc):
    """Assert that document history is squashed after the document TTL."""
    store_name = "my_store"
    ystore = MySQLiteYStore(store_name, delete_db=True)

    for i in range(3):
        # assert that adding a record before document TTL doesn't delete document history
        await ystore.write(test_ydoc.update())
        async with aiosqlite.connect(ystore.db_path) as db:
            assert (await count_yupdates(db)) == i + 1

    await ystore._squash_task

    async with aiosqlite.connect(ystore.db_path) as db:
        assert (await count_yupdates(db)) == 1


@pytest.mark.asyncio
async def test_document_ttl_simultaneous_write_sqlite_ystore(test_ydoc):
    """Assert that document history is squashed after the document TTL, and a
    write that happens at the same time is also squashed."""
    store_name = "my_store"
    ystore = MySQLiteYStore(store_name, delete_db=True)

    for i in range(3):
        await ystore.write(test_ydoc.update())
        async with aiosqlite.connect(ystore.db_path) as db:
            assert (await count_yupdates(db)) == i + 1

    await asyncio.sleep(ystore.document_ttl)
    await ystore.write(test_ydoc.update())
    await ystore._squash_task

    async with aiosqlite.connect(ystore.db_path) as db:
        assert (await count_yupdates(db)) == 1


@pytest.mark.asyncio
async def test_document_ttl_init_sqlite_ystore(test_ydoc):
    """Assert that document history is squashed on init if the document TTL has
    already elapsed since last update."""
    store_name = "my_store"
    ystore = MySQLiteYStore(store_name, delete_db=True)
    now = time.time()

    with patch("time.time") as mock_time:
        mock_time.return_value = now - ystore.document_ttl - 1
        for i in range(3):
            await ystore.write(test_ydoc.update())
            async with aiosqlite.connect(ystore.db_path) as db:
                assert (await count_yupdates(db)) == i + 1

    del ystore
    ystore = MySQLiteYStore(store_name)
    await ystore.db_initialized

    async with aiosqlite.connect(ystore.db_path) as db:
        assert (await count_yupdates(db)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("YStore", (MyTempFileYStore, MySQLiteYStore))
async def test_version(YStore, caplog):
    store_name = "my_store"
    prev_version = YStore.version
    YStore.version = -1
    ystore = YStore(store_name)
    await ystore.write(b"foo")
    YStore.version = prev_version
    assert "YStore version mismatch" in caplog.text
