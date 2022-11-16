import asyncio
from types import TracebackType
from typing import Optional, Type

import y_py as Y

from .yutils import create_update_message


class YDoc(Y.YDoc):
    """A YDoc with a custom transaction context manager that allows to put updates into a queue.
    `Y.YDoc.observe_after_transaction` catches all updates, while updates made through this YDoc
    can be processed separately, which is useful for e.g. updates made only from the backend.
    """

    _begin_transaction = Y.YDoc.begin_transaction
    _update_queue: asyncio.Queue
    _ready: bool

    def init(self, update_queue: asyncio.Queue):
        self._ready = False
        self._update_queue = update_queue

    def begin_transaction(self):
        return Transaction(self, self._update_queue, self._ready)


class Transaction:

    ydoc: YDoc
    update_queue: asyncio.Queue
    state: bytes
    transaction: Y.YTransaction
    ready: bool

    def __init__(self, ydoc: YDoc, update_queue: asyncio.Queue, ready: bool):
        self.ydoc = ydoc
        self.update_queue = update_queue
        self.ready = ready

    def __enter__(self):
        if self.ready:
            self.state = Y.encode_state_vector(self.ydoc)
        self.transaction = self.ydoc._begin_transaction()
        return self.transaction.__enter__()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        res = self.transaction.__exit__(exc_type, exc_value, exc_tb)  # type: ignore
        if self.ready:
            update = Y.encode_state_as_update(self.ydoc, self.state)
            message = create_update_message(update)
            self.update_queue.put_nowait(message)
        return res
