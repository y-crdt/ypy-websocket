from __future__ import annotations

import time
import struct
import tempfile
from logging import Logger, getLogger
from pathlib import Path
from typing import AsyncIterator, Awaitable, Callable

import anyio
from anyio import Lock

import y_py as Y

from .yutils import Decoder, get_new_path, write_var_uint
from .base_store import BaseYStore
from .utils import YDocNotFound

class FileYStore(BaseYStore):
    """A YStore which uses one file per document."""

    path: str
    metadata_callback: Callable[[], Awaitable[bytes] | bytes] | None
    lock: Lock

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

    async def check_version(self) -> int:
        """Check the version of the store format.

        Returns:
            The offset where the data is located in the file.
        """
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
        """Async iterator for reading the store content.

        Returns:
            A tuple of (update, metadata, timestamp) for each update.
        """
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
        """Store an update.

        Arguments:
            data: The update to store.
        """
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
    """
    A YStore which uses the system's temporary directory.
    Files are writen under a common directory.
    To prefix the directory name (e.g. /tmp/my_prefix_b4whmm7y/):

    ```py
    class PrefixTempFileYStore(TempFileYStore):
        prefix_dir = "my_prefix_"
    ```
    """
    
    prefix_dir: str | None = None
    base_dir: str | None = None

    def __init__(
        self,
        path: str,
        metadata_callback: Callable[[], Awaitable[bytes] | bytes] | None = None,
        log: Logger | None = None,
    ):
        """Initialize the object.

        Arguments:
            path: The file path used to store the updates.
            metadata_callback: An optional callback to call to get the metadata.
            log: An optional logger.
        """
        full_path = str(Path(self.get_base_dir()) / path)
        super().__init__(full_path, metadata_callback=metadata_callback, log=log)

    def get_base_dir(self) -> str:
        """Get the base directory where the update file is written.

        Returns:
            The base directory path.
        """
        if self.base_dir is None:
            self.make_directory()
        assert self.base_dir is not None
        return self.base_dir

    def make_directory(self):
        """Create the base directory where the update file is written."""
        type(self).base_dir = tempfile.mkdtemp(prefix=self.prefix_dir)
