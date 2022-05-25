import asyncio
import tempfile
from pathlib import Path
from typing import AsyncIterator, Optional

import aiofiles  # type: ignore
import aiosqlite  # type: ignore
import y_py as Y

from .yutils import get_messages, write_var_uint


class BaseYStore:
    def __init__(self, path: str):
        raise RuntimeError("Not implemented")

    async def write(self, data: bytes):
        raise RuntimeError("Not implemented")

    async def read(self) -> AsyncIterator[bytes]:
        raise RuntimeError("Not implemented")
        yield b""

    async def encode_state_as_update(self, ydoc: Y.YDoc):
        update = Y.encode_state_as_update(ydoc)  # type: ignore
        await self.write(bytes(update))

    async def apply_updates(self, ydoc: Y.YDoc):
        async for update in self.read():
            Y.apply_update(ydoc, update)  # type: ignore


class FileYStore(BaseYStore):
    """A YStore which uses the local file system."""

    path: str

    def __init__(self, path: str):
        self.path = path

    async def read(self) -> AsyncIterator[bytes]:
        async with aiofiles.open(self.path, "rb") as f:
            data = await f.read()
        for update in get_messages(data):
            yield update

    async def write(self, data: bytes):
        var_len = bytes(write_var_uint(len(data)))
        parent = Path(self.path).parent
        if not parent.exists():
            parent.mkdir(parents=True)
            mode = "wb"
        else:
            mode = "ab"
        async with aiofiles.open(self.path, mode) as f:
            await f.write(var_len + data)


class TempFileYStore(FileYStore):
    """A YStore which uses the system's temporary directory.
    To prefix the directory name (e.g. /tmp/my_prefix_b4whmm7y/):

    class PrefixTempFileYStore(TempFileYStore):
        prefix_dir = "my_prefix_"
    """

    prefix_dir: Optional[str] = None
    _base_dir: Optional[str] = None

    def __init__(self, path: str):
        full_path = str(Path(self.base_dir) / path)
        super().__init__(full_path)

    @property
    def base_dir(self) -> str:
        if self._base_dir is None:
            self.make_directory()
        assert self._base_dir is not None
        return self._base_dir

    def make_directory(self):
        type(self)._base_dir = tempfile.mkdtemp(prefix=self.prefix_dir)


class SQLiteYStore(BaseYStore):
    """A YStore which uses an SQLite database.
    Subclass to point to your database file:

    class MySQLiteYStore(SQLiteYStore):
        db_path = "path/to/my_ystore.db"
    """

    db_path: str = "ystore.db"
    path: str
    db_created: asyncio.Event

    def __init__(self, path: str):
        self.path = path
        self.db_created = asyncio.Event()
        asyncio.create_task(self.create_db())

    async def create_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("CREATE TABLE IF NOT EXISTS yupdates (path TEXT, yupdate BLOB)")
            await db.commit()
        self.db_created.set()

    async def read(self) -> AsyncIterator[bytes]:
        await self.db_created.wait()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT * FROM yupdates WHERE path = ?", (self.path,)) as cursor:
                async for _, update in cursor:
                    yield update

    async def write(self, data: bytes):
        await self.db_created.wait()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT INTO yupdates VALUES (?, ?)", (self.path, data))
            await db.commit()
