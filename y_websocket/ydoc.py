import asyncio
from types import TracebackType
from typing import Optional, Type

import y_py as Y

from .yutils import create_update_message


class YDoc(Y.YDoc):

    _update_queue: asyncio.Queue

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._update_queue = asyncio.Queue()

    _begin_transaction = Y.YDoc.begin_transaction

    def begin_transaction(self):
        return Transaction(self, self._update_queue)


class Transaction:

    ydoc: YDoc
    update_queue: asyncio.Queue

    def __init__(self, ydoc: YDoc, update_queue: asyncio.Queue):
        self.ydoc = ydoc
        self.update_queue = update_queue

    def __enter__(self):
        self.state = Y.encode_state_vector(self.ydoc)
        self.transaction = self.ydoc._begin_transaction()
        return self.transaction.__enter__()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ):
        res = self.transaction.__exit__(exc_type, exc_value, exc_tb)
        update = Y.encode_state_as_update(self.ydoc, self.state)
        message = create_update_message(update)
        self.update_queue.put_nowait(message)
        return res
