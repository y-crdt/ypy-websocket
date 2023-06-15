from __future__ import annotations

from contextlib import AsyncExitStack
from functools import partial
from logging import Logger, getLogger
from typing import Callable

import y_py as Y
from anyio import Event, create_memory_object_stream, create_task_group
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from .awareness import Awareness
from .ystore import BaseYStore
from .yutils import create_update_message, put_updates


class YRoom:

    clients: list
    ydoc: Y.YDoc
    ystore: BaseYStore | None
    _on_message: Callable[[bytes], bool] | None
    _update_send_stream: MemoryObjectSendStream
    _update_receive_stream: MemoryObjectReceiveStream
    _ready: bool
    _task_group: TaskGroup | None
    _started: Event | None

    def __init__(
        self, ready: bool = True, ystore: BaseYStore | None = None, log: Logger | None = None
    ):
        """Initialize the object.

        The YRoom instance should preferably be used as an async context manager:
        ```py
        async with room:
            ...
        ```
        However, a lower-level API can also be used:
        ```py
        task = asyncio.create_task(room.start())
        await room.started.wait()
        ...
        room.stop()
        ```

        Arguments:
            ready: Whether the internal YDoc is ready to be synchronized right away.
            ystore: An optional store in which to persist document updates.
            log: An optional logger.
        """
        self.ydoc = Y.YDoc()
        self.awareness = Awareness(self.ydoc)
        self._update_send_stream, self._update_receive_stream = create_memory_object_stream(
            max_buffer_size=65536
        )
        self._ready = False
        self.ready = ready
        self.ystore = ystore
        self.log = log or getLogger(__name__)
        self.clients = []
        self._on_message = None
        self._started = None
        self._task_group = None

    @property
    def started(self):
        """An async event that is set when the WebSocket provider has started."""
        if self._started is None:
            self._started = Event()
        return self._started

    @property
    def ready(self) -> bool:
        """
        Returns:
            True is the internal YDoc is ready to be synchronized.
        """
        return self._ready

    @ready.setter
    def ready(self, value: bool) -> None:
        """Arguments:
        value: True if the internal YDoc is ready to be synchronized, False otherwise."""
        self._ready = value
        if value:
            self.ydoc.observe_after_transaction(partial(put_updates, self._update_send_stream))

    @property
    def on_message(self) -> Callable[[bytes], bool] | None:
        """
        Returns:
            The callback called when a message was received, if any.
        """
        return self._on_message

    @on_message.setter
    def on_message(self, value: Callable[[bytes], bool] | None):
        """Arguments:
        value: the callback to call when a message is received. If the callback returns True, the message is skipped.
        """
        self._on_message = value

    async def _broadcast_updates(self):
        async with self._update_receive_stream:
            async for update in self._update_receive_stream:
                if self._task_group.cancel_scope.cancel_called:
                    return
                # broadcast internal ydoc's update to all clients, that includes changes from the
                # clients and changes from the backend (out-of-band changes)
                for client in self.clients:
                    self.log.debug("Sending Y update to client with endpoint: %s", client.path)
                    message = create_update_message(update)
                    self._task_group.start_soon(client.send, message)
                if self.ystore:
                    self.log.debug("Writing Y update to YStore")
                    self._task_group.start_soon(self.ystore.write, update)

    async def __aenter__(self) -> YRoom:
        if self._task_group is not None:
            raise RuntimeError("YRoom already running")

        async with AsyncExitStack() as exit_stack:
            tg = create_task_group()
            self._task_group = await exit_stack.enter_async_context(tg)
            self._exit_stack = exit_stack.pop_all()
            tg.start_soon(self._broadcast_updates)
            self.started.set()

        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if self._task_group is None:
            raise RuntimeError("YRoom not running")

        self._task_group.cancel_scope.cancel()
        self._task_group = None
        return await self._exit_stack.__aexit__(exc_type, exc_value, exc_tb)

    async def start(self):
        """Start the room."""
        if self._task_group is not None:
            raise RuntimeError("YRoom already running")

        async with create_task_group() as self._task_group:
            self._task_group.start_soon(self._broadcast_updates)
            self.started.set()

    def stop(self):
        """Stop the room."""
        if self._task_group is None:
            raise RuntimeError("YRoom not running")

        self._task_group.cancel_scope.cancel()
        self._task_group = None
