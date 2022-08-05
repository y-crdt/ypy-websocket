import asyncio
import tempfile
from pathlib import Path
from typing import AsyncIterator, Callable, Optional, Tuple

import aiofiles  # type: ignore
import aiosqlite  # type: ignore
import y_py as Y

from .yutils import Decoder, write_var_uint


class YDocNotFound(Exception):
    pass


class BaseYStore:

    metadata_callback: Optional[Callable] = None

    def __init__(self, path: str, metadata_callback=None):
        raise RuntimeError("Not implemented")

    async def write(self, data: bytes) -> None:
        raise RuntimeError("Not implemented")

    async def read(self) -> AsyncIterator[Tuple[bytes, bytes]]:
        raise RuntimeError("Not implemented")
        yield b"", b""

    async def get_metadata(self) -> bytes:
        metadata = b"" if not self.metadata_callback else await self.metadata_callback()
        return metadata

    async def encode_state_as_update(self, ydoc: Y.YDoc):
        update = Y.encode_state_as_update(ydoc)  # type: ignore
        await self.write(update)

    async def apply_updates(self, ydoc: Y.YDoc):
        async for update, metadata in self.read():
            Y.apply_update(ydoc, update)  # type: ignore


class FileYStore(BaseYStore):
    """A YStore which uses the local file system."""

    path: str

    def __init__(self, path: str, metadata_callback=None):
        self.path = path
        self.metadata_callback = metadata_callback

    async def read(self) -> AsyncIterator[Tuple[bytes, bytes]]:
        try:
            async with aiofiles.open(self.path, "rb") as f:
                data = await f.read()
        except Exception:
            raise YDocNotFound
        is_data = True
        for d in Decoder(data).read_messages():
            if is_data:
                update = d
            else:
                yield update, d
            is_data = not is_data

    async def write(self, data: bytes) -> None:
        parent = Path(self.path).parent
        if not parent.exists():
            parent.mkdir(parents=True)
            mode = "wb"
        else:
            mode = "ab"
        async with aiofiles.open(self.path, mode) as f:
            data_len = write_var_uint(len(data))
            await f.write(data_len + data)
            metadata = await self.get_metadata()
            metadata_len = write_var_uint(len(metadata))
            await f.write(metadata_len + metadata)


class TempFileYStore(FileYStore):
    """A YStore which uses the system's temporary directory.
    To prefix the directory name (e.g. /tmp/my_prefix_b4whmm7y/):

    class PrefixTempFileYStore(TempFileYStore):
        prefix_dir = "my_prefix_"
    """

    prefix_dir: Optional[str] = None
    base_dir: Optional[str] = None

    def __init__(self, path: str, metadata_callback=None):
        full_path = str(Path(self.get_base_dir()) / path)
        super().__init__(full_path, metadata_callback=metadata_callback)

    def get_base_dir(self) -> str:
        if self.base_dir is None:
            self.make_directory()
        assert self.base_dir is not None
        return self.base_dir

    def make_directory(self):
        type(self).base_dir = tempfile.mkdtemp(prefix=self.prefix_dir)


class SQLiteYStore(BaseYStore):
    """A YStore which uses an SQLite database.
    Subclass to point to your database file:

    class MySQLiteYStore(SQLiteYStore):
        db_path = "path/to/my_ystore.db"
    """

    db_path: str = "ystore.db"
    path: str
    db_created: asyncio.Event

    def __init__(self, path: str, metadata_callback=None):
        self.path = path
        self.metadata_callback = metadata_callback
        self.db_created = asyncio.Event()
        asyncio.create_task(self.create_db())

    async def create_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("CREATE TABLE IF NOT EXISTS yupdates (path TEXT, yupdate BLOB)")
            await db.commit()
        self.db_created.set()

    async def read(self) -> AsyncIterator[Tuple[bytes, bytes]]:
        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute(
                    "SELECT * FROM yupdates WHERE path = ?", (self.path,)
                ) as cursor:
                    found = False
                    is_data = True
                    async for _, d in cursor:
                        found = True
                        if is_data:
                            update = d
                        else:
                            yield update, d
                        is_data = not is_data
                    if not found:
                        raise YDocNotFound
        except Exception:
            raise YDocNotFound

    async def write(self, data: bytes) -> None:
        await self.db_created.wait()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT INTO yupdates VALUES (?, ?)", (self.path, data))
            metadata = await self.get_metadata()
            await db.execute("INSERT INTO yupdates VALUES (?, ?)", (self.path, metadata))
            await db.commit()
