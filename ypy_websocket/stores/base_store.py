from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AsyncExitStack
from inspect import isawaitable
from typing import AsyncIterator, Awaitable, Callable, cast

import y_py as Y
from anyio import TASK_STATUS_IGNORED, Event, create_task_group
from anyio.abc import TaskGroup, TaskStatus


class BaseYStore(ABC):
    """
    Base class for the stores.
    """

    version = 3
    metadata_callback: Callable[[], Awaitable[bytes] | bytes] | None = None

    _store_path: str
    _initialized: Event | None = None
    _started: Event | None = None
    _starting: bool = False
    _task_group: TaskGroup | None = None

    @abstractmethod
    def __init__(
        self, path: str, metadata_callback: Callable[[], Awaitable[bytes] | bytes] | None = None
    ):
        """
        Initialize the object.

        Arguments:
            path: The path where the store will be located or the prefix for file-based stores.
            metadata_callback: An optional callback to call to get the metadata.
            log: An optional logger.
        """
        ...

    @abstractmethod
    async def initialize(self) -> None:
        """
        Initializes the store.
        """
        ...

    @abstractmethod
    async def exists(self, path: str) -> bool:
        """
        Returns True if the document exists, else returns False.

        Arguments:
            path: The document name/path.
        """
        ...

    @abstractmethod
    async def list(self) -> AsyncIterator[str]:
        """
        Returns a list with the name/path of the documents stored.
        """
        ...

    @abstractmethod
    async def get(self, path: str, updates: bool = False) -> dict | None:
        """
        Returns the document's metadata or None if the document does't exist.

        Arguments:
            path: The document name/path.
        """
        ...

    @abstractmethod
    async def create(self, path: str, version: int) -> None:
        """
        Creates a new document.

        Arguments:
            path: The document name/path.
            version: Document version.
        """
        ...

    @abstractmethod
    async def remove(self, path: str) -> dict | None:
        """
        Removes a document.

        Arguments:
            path: The document name/path.
        """
        ...

    @abstractmethod
    async def write(self, path: str, data: bytes) -> None:
        """
        Store a document update.

        Arguments:
            path: The document name/path.
            data: The update to store.
        """
        ...

    @abstractmethod
    async def read(self, path: str) -> AsyncIterator[tuple[bytes, bytes]]:
        """
        Async iterator for reading document's updates.

        Arguments:
            path: The document name/path.

        Returns:
            A tuple of (update, metadata, timestamp) for each update.
        """
        ...

    @property
    def initialized(self) -> bool:
        if self._initialized is not None:
            return self._initialized.is_set()
        else:
            return False

    @property
    def started(self) -> Event:
        if self._started is None:
            self._started = Event()
        return self._started

    async def __aenter__(self) -> BaseYStore:
        if self._task_group is not None:
            raise RuntimeError("YStore already running")

        async with AsyncExitStack() as exit_stack:
            tg = create_task_group()
            self._task_group = await exit_stack.enter_async_context(tg)
            self._exit_stack = exit_stack.pop_all()
            tg.start_soon(self.start)

        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if self._task_group is None:
            raise RuntimeError("YStore not running")

        self._task_group.cancel_scope.cancel()
        self._task_group = None
        return await self._exit_stack.__aexit__(exc_type, exc_value, exc_tb)

    async def start(self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        """Start the store.

        Arguments:
            task_status: The status to set when the task has started.
        """
        if self._starting:
            return
        else:
            self._staring = True

        if self._task_group is not None:
            raise RuntimeError("YStore already running")

        self.started.set()
        self._starting = False
        task_status.started()

    def stop(self) -> None:
        """Stop the store."""
        if self._task_group is None:
            raise RuntimeError("YStore not running")

        self._task_group.cancel_scope.cancel()
        self._task_group = None

    async def get_metadata(self) -> bytes:
        """
        Returns:
            The metadata.
        """
        if self.metadata_callback is None:
            return b""

        metadata = self.metadata_callback()
        if isawaitable(metadata):
            metadata = await metadata
        metadata = cast(bytes, metadata)
        return metadata

    async def encode_state_as_update(self, path: str, ydoc: Y.YDoc) -> None:
        """Store a YDoc state.

        Arguments:
            path: The document name/path.
            ydoc: The YDoc from which to store the state.
        """
        update = Y.encode_state_as_update(ydoc)  # type: ignore
        await self.write(path, update)

    async def apply_updates(self, path: str, ydoc: Y.YDoc) -> None:
        """Apply all stored updates to the YDoc.

        Arguments:
            path: The document name/path.
            ydoc: The YDoc on which to apply the updates.
        """
        async for update, *rest in self.read(path):  # type: ignore
            Y.apply_update(ydoc, update)  # type: ignore
