import struct
import time
from pathlib import Path

import anyio
import pytest

from ypy_websocket.stores import DocExists, FileYStore
from ypy_websocket.yutils import Decoder, write_var_uint


@pytest.fixture
def create_store():
    async def _inner(path: str, version: int) -> None:
        await anyio.Path(path).mkdir(parents=True, exist_ok=True)
        version_path = Path(path, "__version__")
        async with await anyio.open_file(version_path, "wb") as f:
            version_bytes = str(version).encode()
            await f.write(version_bytes)

    return _inner


@pytest.fixture
def add_document():
    async def _inner(path: str, doc_path: str, version: int, data: bytes = b"") -> None:
        file_path = Path(path, (doc_path + ".y"))
        await anyio.Path(file_path.parent).mkdir(parents=True, exist_ok=True)

        async with await anyio.open_file(file_path, "ab") as f:
            version_bytes = f"VERSION:{version}\n".encode()
            await f.write(version_bytes)
            data_len = write_var_uint(len(data))
            await f.write(data_len + data)
            metadata = b""
            metadata_len = write_var_uint(len(metadata))
            await f.write(metadata_len + metadata)
            timestamp = struct.pack("<d", time.time())
            timestamp_len = write_var_uint(len(timestamp))
            await f.write(timestamp_len + timestamp)

    return _inner


@pytest.mark.anyio
async def test_initialization(tmp_path):
    path = tmp_path / "tmp"
    store = FileYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    version_path = Path(path / "__version__")
    async with await anyio.open_file(version_path, "rb") as f:
        version = int(await f.readline())
        assert FileYStore.version == version


@pytest.mark.anyio
async def test_initialization_with_old_store(tmp_path, create_store):
    path = tmp_path / "tmp"

    # Create a store with an old version
    await create_store(path, 1)

    store = FileYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    version_path = Path(path / "__version__")
    async with await anyio.open_file(version_path, "rb") as f:
        version = int(await f.readline())
        assert FileYStore.version == version


@pytest.mark.anyio
async def test_initialization_with_existing_store(tmp_path, create_store, add_document):
    path = tmp_path / "tmp"
    doc_path = "test.txt"

    # Create a store with an old version
    await create_store(path, FileYStore.version)
    await add_document(path, doc_path, 0)

    store = FileYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    version_path = Path(path / "__version__")
    async with await anyio.open_file(version_path, "rb") as f:
        version = int(await f.readline())
        assert FileYStore.version == version

    file_path = Path(path / (doc_path + ".y"))
    assert await anyio.Path(file_path).exists()


@pytest.mark.anyio
async def test_exists(tmp_path, create_store, add_document):
    path = tmp_path / "tmp"
    doc_path = "test.txt"

    # Create a store with an old version
    await create_store(path, FileYStore.version)
    await add_document(path, doc_path, 0)

    store = FileYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    assert await store.exists(doc_path)

    assert not await store.exists("random.path")


@pytest.mark.anyio
async def test_list(tmp_path, create_store, add_document):
    path = tmp_path / "tmp"
    doc1 = "test_1.txt"
    doc2 = "path/to/dir/test_2.txt"

    # Create a store with an old version
    await create_store(path, FileYStore.version)
    await add_document(path, doc1, 0)
    await add_document(path, doc2, 0)

    store = FileYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    count = 0
    async for doc in store.list():
        count += 1
        # assert doc in [doc1, doc2]

    assert count == 2


@pytest.mark.anyio
async def test_get(tmp_path, create_store, add_document):
    path = tmp_path / "tmp"
    doc_path = "test.txt"

    # Create a store with an old version
    await create_store(path, FileYStore.version)
    await add_document(path, doc_path, 0)

    store = FileYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    res = await store.get(doc_path)
    assert res["path"] == doc_path
    assert res["version"] == 0

    res = await store.get("random.doc")
    assert res is None


@pytest.mark.anyio
async def test_create(tmp_path, create_store, add_document):
    path = tmp_path / "tmp"
    doc_path = "test.txt"

    # Create a store with an old version
    await create_store(path, FileYStore.version)
    await add_document(path, doc_path, 0)

    store = FileYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    new_doc = "new_doc.path"
    await store.create(new_doc, 0)

    file_path = Path(path / (new_doc + ".y"))
    async with await anyio.open_file(file_path, "rb") as f:
        header = await f.read(8)
        assert header == b"VERSION:"

        version = int(await f.readline())
        assert version == 0

    with pytest.raises(DocExists) as e:
        await store.create(doc_path, 0)
    assert str(e.value) == f"The document {doc_path} already exists."


@pytest.mark.anyio
async def test_remove(tmp_path, create_store, add_document):
    path = tmp_path / "tmp"
    doc_path = "test.txt"

    # Create a store with an old version
    await create_store(path, FileYStore.version)
    await add_document(path, doc_path, 0)

    store = FileYStore(str(path))
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
async def test_read(tmp_path, create_store, add_document):
    path = tmp_path / "tmp"
    doc_path = "test.txt"
    update = b"foo"

    # Create a store with an old version
    await create_store(path, FileYStore.version)
    await add_document(path, doc_path, 0, update)

    store = FileYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    count = 0
    async for u, _, _ in store.read(doc_path):
        count += 1
        assert update == u

    assert count == 1


@pytest.mark.anyio
async def test_write(tmp_path, create_store, add_document):
    path = tmp_path / "tmp"
    doc_path = "test.txt"

    # Create a store with an old version
    await create_store(path, FileYStore.version)
    await add_document(path, doc_path, 0)

    store = FileYStore(str(path))
    await store.start()
    await store.initialize()

    assert store.initialized

    update = b"foo"
    await store.write(doc_path, update)

    file_path = Path(path / (doc_path + ".y"))
    async with await anyio.open_file(file_path, "rb") as f:
        header = await f.read(8)
        assert header == b"VERSION:"

        version = int(await f.readline())
        assert version == 0

        count = 0
        data = await f.read()

        i = 0
        for u in Decoder(data).read_messages():
            if i == 0:
                count += 1
                # The fixture add_document inserts an empty update
                assert u in [b"", update]
            i = (i + 1) % 3

        assert count == 2
