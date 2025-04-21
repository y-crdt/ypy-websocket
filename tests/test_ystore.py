import time
from pathlib import Path
from unittest.mock import patch

import aiosqlite
import pytest

from ypy_websocket.stores import SQLiteYStore, TempFileYStore


class MetadataCallback:
    def __init__(self):
        self.i = 0

    async def __call__(self):
        res = str(self.i).encode()
        self.i += 1
        return res


class MyTempFileYStore(TempFileYStore):
    prefix_dir = "test_temp_"


class MySQLiteYStore(SQLiteYStore):
    document_ttl = 1000


@pytest.mark.anyio
@pytest.mark.parametrize("YStore", (MyTempFileYStore, MySQLiteYStore))
async def test_ystore(tmp_path, YStore):
    store_path = tmp_path / "my_store"
    doc_name = "my_doc.txt"

    ystore = YStore(str(store_path), metadata_callback=MetadataCallback())
    await ystore.initialize()

    await ystore.create(doc_name, 0)

    data = [b"foo", b"bar", b"baz"]
    for d in data:
        await ystore.write(doc_name, d)

    if YStore == MyTempFileYStore:
        assert (Path(store_path) / (doc_name + ".y")).exists()
    elif YStore == MySQLiteYStore:
        assert Path(store_path).exists()
    i = 0
    async for d, m, t in ystore.read(doc_name):
        assert d == data[i]  # data
        assert m == str(i).encode()  # metadata
        i += 1

    assert i == len(data)


@pytest.mark.anyio
async def test_document_ttl_sqlite_ystore(tmp_path, test_ydoc):
    store_path = tmp_path / "my_store.db"
    doc_name = "my_doc.txt"

    ystore = MySQLiteYStore(str(store_path))
    await ystore.initialize()

    await ystore.create(doc_name, 0)

    now = time.time()

    for i in range(3):
        # assert that adding a record before document TTL doesn't delete document history
        with patch("time.time") as mock_time:
            mock_time.return_value = now
            await ystore.write(doc_name, test_ydoc.update())
            async with aiosqlite.connect(store_path) as db:
                assert (await (await db.execute("SELECT count(*) FROM yupdates")).fetchone())[
                    0
                ] == i + 1

    # assert that adding a record after document TTL deletes previous document history
    with patch("time.time") as mock_time:
        mock_time.return_value = now + ystore.document_ttl + 1
        await ystore.write(doc_name, test_ydoc.update())
        async with aiosqlite.connect(store_path) as db:
            # two updates in DB: one squashed update and the new update
            assert (await (await db.execute("SELECT count(*) FROM yupdates")).fetchone())[0] == 2
