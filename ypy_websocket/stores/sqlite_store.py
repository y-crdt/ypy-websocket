from __future__ import annotations

import time
import aiosqlite
from logging import Logger, getLogger
from typing import AsyncIterator, Awaitable, Callable


import anyio
from anyio import TASK_STATUS_IGNORED, Event, Lock, create_task_group
from anyio.abc import TaskStatus

import y_py as Y

from .yutils import get_new_path
from .base_store import BaseYStore
from .utils import YDocNotFound

class SQLiteYStore(BaseYStore):
    """A YStore which uses an SQLite database.
    Unlike file-based YStores, the Y updates of all documents are stored in the same database.

    Subclass to point to your database file:

    ```py
    class MySQLiteYStore(SQLiteYStore):
        db_path = "path/to/my_ystore.db"
    ```
    """

    db_path: str = "ystore.db"
    # Determines the "time to live" for all documents, i.e. how recent the
    # latest update of a document must be before purging document history.
    # Defaults to never purging document history (None).
    document_ttl: int | None = None
    path: str
    lock: Lock
    db_initialized: Event

    def __init__(
        self,
        path: str,
        metadata_callback: Callable[[], Awaitable[bytes] | bytes] | None = None,
        log: Logger | None = None,
    ) -> None:
        """Initialize the object.

        Arguments:
            path: The file path used to store the updates.
            metadata_callback: An optional callback to call to get the metadata.
            log: An optional logger.
        """
        self.path = path
        self.metadata_callback = metadata_callback
        self.log = log or getLogger(__name__)
        self.lock = Lock()
        self.db_initialized = Event()

    async def start(self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        """Start the SQLiteYStore.

        Arguments:
            task_status: The status to set when the task has started.
        """
        if self._starting:
            return
        else:
            self._starting = True

        if self._task_group is not None:
            raise RuntimeError("YStore already running")

        async with create_task_group() as self._task_group:
            self._task_group.start_soon(self._init_db)
            self.started.set()
            self._starting = False
            task_status.started()

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
        """Async iterator for reading the store content.

        Returns:
            A tuple of (update, metadata, timestamp) for each update.
        """
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
        """Store an update.

        Arguments:
            data: The update to store.
        """
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
