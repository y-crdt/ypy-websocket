from __future__ import annotations

import logging
import struct
import tempfile
import time
from abc import ABC, abstractmethod
from contextlib import AsyncExitStack
from pathlib import Path
from typing import AsyncIterator, Callable, Optional, Tuple

import aiosqlite
import anyio
import y_py as Y
from anyio import Event, Lock, create_task_group
from anyio.abc import TaskGroup

from .yutils import Decoder, get_new_path, write_var_uint


class YDocNotFound(Exception):
    pass


class BaseYStore(ABC):

    metadata_callback: Callable | None = None
    version = 2
    _started: Event | None = None
    _task_group: TaskGroup | None = None

    @abstractmethod
    def __init__(self, path: str, metadata_callback=None):
        ...

    @abstractmethod
    async def write(self, data: bytes) -> None:
        ...

    @abstractmethod
    async def read(self) -> AsyncIterator[tuple[bytes, bytes]]:
        ...

    @property
    def started(self) -> Event:
        if self._started is None:
            self._started = Event()
        return self._started

    async def __aenter__(self):
        if self._task_group is not None:
            raise RuntimeError("YStore already running")

        async with AsyncExitStack() as exit_stack:
            tg = create_task_group()
            self._task_group = await exit_stack.enter_async_context(tg)
            self._exit_stack = exit_stack.pop_all()
            tg.start_soon(self.start)

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        self._task_group.cancel_scope.cancel()
        self._task_group = None
        return await self._exit_stack.__aexit__(exc_type, exc_value, exc_tb)

    async def start(self):
        if self._task_group is not None:
            raise RuntimeError("YStore already running")

        self.started.set()

    def stop(self):
        self._task_group.cancel_scope.cancel()
        self._task_group = None

    async def get_metadata(self) -> bytes:
        metadata = b"" if not self.metadata_callback else await self.metadata_callback()
        return metadata

    async def encode_state_as_update(self, ydoc: Y.YDoc):
        update = Y.encode_state_as_update(ydoc)  # type: ignore
        await self.write(update)

    async def apply_updates(self, ydoc: Y.YDoc):
        async for update, *rest in self.read():  # type: ignore
            Y.apply_update(ydoc, update)  # type: ignore


class FileYStore(BaseYStore):
    """A YStore which uses one file per document."""

    path: str
    metadata_callback: Callable | None
    lock: Lock

    def __init__(self, path: str, metadata_callback: Callable | None = None, log=None):
        self.path = path
        self.metadata_callback = metadata_callback
        self.log = log or logging.getLogger(__name__)
        self.lock = Lock()

    async def check_version(self) -> int:
        if not await anyio.Path(self.path).exists():
            version_mismatch = True
        else:
            version_mismatch = False
            move_file = False
            async with await anyio.open_file(self.path, "rb") as f:
                header = await f.read(8)
                if header == b"VERSION:":
                    version = int(await f.readline())
                    if version == self.version:
                        offset = await f.tell()
                    else:
                        version_mismatch = True
                else:
                    version_mismatch = True
                if version_mismatch:
                    move_file = True
            if move_file:
                new_path = await get_new_path(self.path)
                self.log.warning(f"YStore version mismatch, moving {self.path} to {new_path}")
                await anyio.Path(self.path).rename(new_path)
        if version_mismatch:
            async with await anyio.open_file(self.path, "wb") as f:
                version_bytes = f"VERSION:{self.version}\n".encode()
                await f.write(version_bytes)
                offset = len(version_bytes)
        return offset

    async def read(self) -> AsyncIterator[tuple[bytes, bytes, float]]:  # type: ignore
        async with self.lock:
            if not await anyio.Path(self.path).exists():
                raise YDocNotFound
            offset = await self.check_version()
            async with await anyio.open_file(self.path, "rb") as f:
                await f.seek(offset)
                data = await f.read()
                if not data:
                    raise YDocNotFound
        i = 0
        for d in Decoder(data).read_messages():
            if i == 0:
                update = d
            elif i == 1:
                metadata = d
            else:
                timestamp = struct.unpack("<d", d)[0]
                yield update, metadata, timestamp
            i = (i + 1) % 3

    async def write(self, data: bytes) -> None:
        parent = Path(self.path).parent
        async with self.lock:
            await anyio.Path(parent).mkdir(parents=True, exist_ok=True)
            await self.check_version()
            async with await anyio.open_file(self.path, "ab") as f:
                data_len = write_var_uint(len(data))
                await f.write(data_len + data)
                metadata = await self.get_metadata()
                metadata_len = write_var_uint(len(metadata))
                await f.write(metadata_len + metadata)
                timestamp = struct.pack("<d", time.time())
                timestamp_len = write_var_uint(len(timestamp))
                await f.write(timestamp_len + timestamp)


class TempFileYStore(FileYStore):
    """A YStore which uses the system's temporary directory.
    Files are writen under a common directory.
    To prefix the directory name (e.g. /tmp/my_prefix_b4whmm7y/):

    class PrefixTempFileYStore(TempFileYStore):
        prefix_dir = "my_prefix_"
    """

    prefix_dir: str | None = None
    base_dir: str | None = None

    def __init__(self, path: str, metadata_callback: Callable | None = None, log=None):
        full_path = str(Path(self.get_base_dir()) / path)
        super().__init__(full_path, metadata_callback=metadata_callback, log=log)

    def get_base_dir(self) -> str:
        if self.base_dir is None:
            self.make_directory()
        assert self.base_dir is not None
        return self.base_dir

    def make_directory(self):
        type(self).base_dir = tempfile.mkdtemp(prefix=self.prefix_dir)


class SQLiteYStore(BaseYStore):
    """A YStore which uses an SQLite database.
    Unlike file-based YStores, the Y updates of all documents are stored in the same database.

    Subclass to point to your database file:

    class MySQLiteYStore(SQLiteYStore):
        db_path = "path/to/my_ystore.db"
    """

    db_path: str = "ystore.db"
    # Determines the "time to live" for all documents, i.e. how recent the
    # latest update of a document must be before purging document history.
    # Defaults to never purging document history (None).
    document_ttl: int | None = None
    path: str
    lock: Lock
    db_initialized: Event

    def __init__(self, path: str, metadata_callback: Callable | None = None, log=None):
        self.path = path
        self.metadata_callback = metadata_callback
        self.log = log or logging.getLogger(__name__)
        self.lock = Lock()
        self.db_initialized = Event()

    async def start(self):
        if self._task_group is not None:
            raise RuntimeError("YStore already running")

        async with create_task_group() as self._task_group:
            self._task_group.start_soon(self._init_db)
            self.started.set()

    async def _init_db(self):
        create_db = False
        move_db = False
        if not await anyio.Path(self.db_path).exists():
            create_db = True
        else:
            async with self.lock:
                async with aiosqlite.connect(self.db_path) as db:
                    cursor = await db.execute(
                        "SELECT count(name) FROM sqlite_master WHERE type='table' and name='yupdates'"
                    )
                    table_exists = (await cursor.fetchone())[0]
                    if table_exists:
                        cursor = await db.execute("pragma user_version")
                        version = (await cursor.fetchone())[0]
                        if version != self.version:
                            move_db = True
                            create_db = True
                    else:
                        create_db = True
        if move_db:
            new_path = await get_new_path(self.db_path)
            self.log.warning(f"YStore version mismatch, moving {self.db_path} to {new_path}")
            await anyio.Path(self.db_path).rename(new_path)
        if create_db:
            async with self.lock:
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute(
                        "CREATE TABLE yupdates (path TEXT NOT NULL, yupdate BLOB, metadata BLOB, timestamp REAL NOT NULL)"
                    )
                    await db.execute(
                        "CREATE INDEX idx_yupdates_path_timestamp ON yupdates (path, timestamp)"
                    )
                    await db.execute(f"PRAGMA user_version = {self.version}")
                    await db.commit()
        self.db_initialized.set()

    async def read(self) -> AsyncIterator[tuple[bytes, bytes, float]]:  # type: ignore
        await self.db_initialized.wait()
        try:
            async with self.lock:
                async with aiosqlite.connect(self.db_path) as db:
                    async with db.execute(
                        "SELECT yupdate, metadata, timestamp FROM yupdates WHERE path = ?",
                        (self.path,),
                    ) as cursor:
                        found = False
                        async for update, metadata, timestamp in cursor:
                            found = True
                            yield update, metadata, timestamp
                        if not found:
                            raise YDocNotFound
        except Exception:
            raise YDocNotFound

    async def write(self, data: bytes) -> None:
        await self.db_initialized.wait()
        async with self.lock:
            async with aiosqlite.connect(self.db_path) as db:
                # first, determine time elapsed since last update
                cursor = await db.execute(
                    "SELECT timestamp FROM yupdates WHERE path = ? ORDER BY timestamp DESC LIMIT 1",
                    (self.path,),
                )
                row = await cursor.fetchone()
                diff = (time.time() - row[0]) if row else 0

                if self.document_ttl is not None and diff > self.document_ttl:
                    # squash updates
                    ydoc = Y.YDoc()
                    async with db.execute(
                        "SELECT yupdate FROM yupdates WHERE path = ?", (self.path,)
                    ) as cursor:
                        async for update, in cursor:
                            Y.apply_update(ydoc, update)
                    # delete history
                    await db.execute("DELETE FROM yupdates WHERE path = ?", (self.path,))
                    # insert squashed updates
                    squashed_update = Y.encode_state_as_update(ydoc)
                    metadata = await self.get_metadata()
                    await db.execute(
                        "INSERT INTO yupdates VALUES (?, ?, ?, ?)",
                        (self.path, squashed_update, metadata, time.time()),
                    )

                # finally, write this update to the DB
                metadata = await self.get_metadata()
                await db.execute(
                    "INSERT INTO yupdates VALUES (?, ?, ?, ?)",
                    (self.path, data, metadata, time.time()),
                )
                await db.commit()
