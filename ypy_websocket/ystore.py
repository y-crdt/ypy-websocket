import tempfile
from pathlib import Path
from typing import Optional

import aiofiles  # type: ignore
import y_py as Y

from .yutils import get_messages, write_var_uint


class BaseYStore:
    def __init__(self, path: str, init: bool = False):
        pass

    async def write(self, data: bytes):
        raise RuntimeError("Not implemented")

    async def read(self) -> bytes:
        raise RuntimeError("Not implemented")

    async def append(self, data: bytes):
        raise RuntimeError("Not implemented")

    async def delete(self):
        raise RuntimeError("Not implemented")

    async def encode_state_as_update(self, ydoc: Y.YDoc):
        update = Y.encode_state_as_update(ydoc)  # type: ignore
        var_len = write_var_uint(len(update))
        await self.append(bytes(var_len + update))

    async def apply_updates(self, ydoc: Y.YDoc):
        updates = await self.read()
        for _, update in get_messages(updates):
            Y.apply_update(ydoc, update)


class FileYStore(BaseYStore):
    """A YStore which uses the local file system."""

    path: str

    def __init__(self, path: str, init: bool = False):
        self.path = path
        if init:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            open(path, "wb").close()

    async def write(self, data: bytes):
        async with aiofiles.open(self.path, "wb") as f:
            await f.write(data)

    async def read(self) -> bytes:
        async with aiofiles.open(self.path, "rb") as f:
            data = await f.read()
        return data

    async def append(self, data: bytes):
        async with aiofiles.open(self.path, "ab") as f:
            await f.write(data)

    async def delete(self):
        Path(self.path).unlink(missing_ok=True)


class TempFileYStore(FileYStore):
    """A YStore which uses the system's temporary directory.
    To prefix the directory name (e.g. /tmp/my_prefix_b4whmm7y/):

    class PrefixTempFileYStore(TempFileYStore):
        prefix_dir = "my_prefix_"
    """

    prefix_dir: Optional[str] = None
    _base_dir: Optional[str] = None

    def __init__(self, path: str, init: bool = False):
        full_path = str(Path(self.base_dir) / path)
        super().__init__(full_path, init)

    @property
    def base_dir(self) -> str:
        if self._base_dir is None:
            self.make_directory()
        assert self._base_dir is not None
        return self._base_dir

    def make_directory(self):
        type(self)._base_dir = tempfile.mkdtemp(prefix=self.prefix_dir)
