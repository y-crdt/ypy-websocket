import time

import aiosqlite
import pytest

from ypy_websocket.stores import DocExists, SQLiteYStore


@pytest.fixture
def create_database():
    async def _inner(path: str, version: int, tables: bool = True) -> None:
        async with aiosqlite.connect(path) as db:
            if tables:
                await db.execute(
                    "CREATE TABLE IF NOT EXISTS documents (path TEXT PRIMARY KEY, version INTEGER NOT NULL)"
                )
                await db.execute(
                    "CREATE TABLE IF NOT EXISTS yupdates (path TEXT NOT NULL, yupdate BLOB, metadata BLOB, timestamp REAL NOT NULL)"
                )
                await db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_yupdates_path_timestamp ON yupdates (path, timestamp)"
                )
            await db.execute(f"PRAGMA user_version = {version}")
            await db.commit()

    return _inner


@pytest.fixture
def add_document():
    async def _inner(path: str, doc_path: str, version: int, data: bytes = b"") -> None:
        async with aiosqlite.connect(path) as db:
            await db.execute(
                "INSERT INTO documents VALUES (?, ?)",
                (doc_path, version),
            )
            await db.execute(
                "INSERT INTO yupdates VALUES (?, ?, ?, ?)",
                (doc_path, data, b"", time.time()),
            )
            await db.commit()

    return _inner


@pytest.mark.anyio
async def test_initialization(tmp_path):
    path = tmp_path / "tmp.db"
    store = SQLiteYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    async with aiosqlite.connect(path) as db:
        cursor = await db.execute("pragma user_version")
        version = (await cursor.fetchone())[0]
        assert store.version == version

        cursor = await db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='documents'"
        )
        res = await cursor.fetchone()
        assert res[0] == 1

        cursor = await db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='yupdates'"
        )
        res = await cursor.fetchone()
        assert res[0] == 1


@pytest.mark.anyio
async def test_initialization_with_old_database(tmp_path, create_database):
    path = tmp_path / "tmp.db"

    # Create a database with an old version
    await create_database(path, 1)

    store = SQLiteYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    async with aiosqlite.connect(path) as db:
        cursor = await db.execute("pragma user_version")
        version = (await cursor.fetchone())[0]
        assert store.version == version

        cursor = await db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='documents'"
        )
        res = await cursor.fetchone()
        assert res[0] == 1

        cursor = await db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='yupdates'"
        )
        res = await cursor.fetchone()
        assert res[0] == 1


@pytest.mark.anyio
async def test_initialization_with_empty_database(tmp_path, create_database):
    path = tmp_path / "tmp.db"

    # Create a database with an old version
    await create_database(path, SQLiteYStore.version, False)

    store = SQLiteYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    async with aiosqlite.connect(path) as db:
        cursor = await db.execute("pragma user_version")
        version = (await cursor.fetchone())[0]
        assert store.version == version

        cursor = await db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='documents'"
        )
        res = await cursor.fetchone()
        assert res[0] == 1

        cursor = await db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='yupdates'"
        )
        res = await cursor.fetchone()
        assert res[0] == 1


@pytest.mark.anyio
async def test_initialization_with_existing_database(tmp_path, create_database, add_document):
    path = tmp_path / "tmp.db"
    doc_path = "test.txt"

    # Create a database with an old version
    await create_database(path, SQLiteYStore.version)
    await add_document(path, doc_path, 0)

    store = SQLiteYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    async with aiosqlite.connect(path) as db:
        cursor = await db.execute("pragma user_version")
        version = (await cursor.fetchone())[0]
        assert store.version == version

        cursor = await db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='documents'"
        )
        res = await cursor.fetchone()
        assert res[0] == 1

        cursor = await db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='yupdates'"
        )
        res = await cursor.fetchone()
        assert res[0] == 1

        cursor = await db.execute(
            "SELECT path, version FROM documents WHERE path = ?",
            (doc_path,),
        )
        res = await cursor.fetchone()
        assert res[0] == doc_path
        assert res[1] == 0


@pytest.mark.anyio
async def test_exists(tmp_path, create_database, add_document):
    path = tmp_path / "tmp.db"
    doc_path = "test.txt"

    # Create a database with an old version
    await create_database(path, SQLiteYStore.version)
    await add_document(path, doc_path, 0)

    store = SQLiteYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    assert await store.exists(doc_path)

    assert not await store.exists("random.path")


@pytest.mark.anyio
async def test_list(tmp_path, create_database, add_document):
    path = tmp_path / "tmp.db"
    doc1 = "test_1.txt"
    doc2 = "test_2.txt"

    # Create a database with an old version
    await create_database(path, SQLiteYStore.version)
    await add_document(path, doc1, 0)
    await add_document(path, doc2, 0)

    store = SQLiteYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    count = 0
    async for doc in store.list():
        count += 1
        assert doc in [doc1, doc2]

    assert count == 2


@pytest.mark.anyio
async def test_get(tmp_path, create_database, add_document):
    path = tmp_path / "tmp.db"
    doc_path = "test.txt"

    # Create a database with an old version
    await create_database(path, SQLiteYStore.version)
    await add_document(path, doc_path, 0)

    store = SQLiteYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    res = await store.get(doc_path)
    assert res["path"] == doc_path
    assert res["version"] == 0

    res = await store.get("random.doc")
    assert res is None


@pytest.mark.anyio
async def test_create(tmp_path, create_database, add_document):
    path = tmp_path / "tmp.db"
    doc_path = "test.txt"

    # Create a database with an old version
    await create_database(path, SQLiteYStore.version)
    await add_document(path, doc_path, 0)

    store = SQLiteYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    new_doc = "new_doc.path"
    await store.create(new_doc, 0)
    async with aiosqlite.connect(path) as db:
        cursor = await db.execute(
            "SELECT path, version FROM documents WHERE path = ?",
            (new_doc,),
        )
        res = await cursor.fetchone()
        assert res[0] == new_doc
        assert res[1] == 0

    with pytest.raises(DocExists) as e:
        await store.create(doc_path, 0)
    assert str(e.value) == f"The document {doc_path} already exists."


@pytest.mark.anyio
async def test_remove(tmp_path, create_database, add_document):
    path = tmp_path / "tmp.db"
    doc_path = "test.txt"

    # Create a database with an old version
    await create_database(path, SQLiteYStore.version)
    await add_document(path, doc_path, 0)

    store = SQLiteYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    await store.remove(doc_path)
    assert not await store.exists(doc_path)

    new_doc = "new_doc.path"
    assert not await store.exists(new_doc)

    await store.remove(new_doc)
    assert not await store.exists(new_doc)


@pytest.mark.anyio
async def test_read(tmp_path, create_database, add_document):
    path = tmp_path / "tmp.db"
    doc_path = "test.txt"
    update = b"foo"

    # Create a database with an old version
    await create_database(path, SQLiteYStore.version)
    await add_document(path, doc_path, 0, update)

    store = SQLiteYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    count = 0
    async for u, _, _ in store.read(doc_path):
        count += 1
        assert update == u

    assert count == 1


@pytest.mark.anyio
async def test_write(tmp_path, create_database, add_document):
    path = tmp_path / "tmp.db"
    doc_path = "test.txt"

    # Create a database with an old version
    await create_database(path, SQLiteYStore.version)
    await add_document(path, doc_path, 0)

    store = SQLiteYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    update = b"foo"
    await store.write(doc_path, update)

    async with aiosqlite.connect(path) as db:
        async with db.execute("SELECT yupdate FROM yupdates WHERE path = ?", (doc_path,)) as cursor:
            count = 0
            async for u, in cursor:
                count += 1
                # The fixture add_document inserts an empty update
                assert u in [b"", update]
            assert count == 2
