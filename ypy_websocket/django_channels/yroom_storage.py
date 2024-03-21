import time
from typing import Optional

import redis.asyncio as redis
import y_py as Y


class BaseYRoomStorage:
    def __init__(self, room_name) -> None:
        self.room_name = room_name

        self.last_saved_at = time.time()
        self.debounce_seconds = 5

    async def get_document(self) -> Y.YDoc:
        """Gets the document from the storage.

        Ideally it should be retrieved first from volatile storage (e.g. Redis) and then from
        persistent storage (e.g. a database).

        Returns:
            The document with the latest changes.
        """

        raise NotImplementedError

    async def update_document(self, update: bytes):
        """Updates the document in the storage.

        Updates could be received by Yjs client (e.g. from a WebSocket) or from the server
        (e.g. from a Django Celery job).

        Args:
            update: The update to apply to the document.
        """

        raise NotImplementedError

    async def persist_document(self) -> None:
        """Persists the document to the storage.

        If you need to persist the document to a database, you should do it here.

        Default implementation does nothing.
        """

        pass

    async def debounced_persist_document(self) -> None:
        """Persists the document to the storage, but debounced."""

        if time.time() - self.last_saved_at <= self.debounce_seconds:
            return

        await self.persist_document()

        self.last_saved_at = time.time()

    async def close(self):
        """Closes the storage.

        Default implementation does nothing.
        """

        pass

    def _apply_update_to_snapshot(self, document: Y.YDoc, update: bytes) -> bytes:
        """Applies an update to a document snapshot.

        Args:
            document: The document snapshot to apply the update to.
            update: The update to apply to the document.

        Returns:
            The updated document snapshot.
        """

        Y.apply_update(document, update)

        return Y.encode_state_as_update(document)




class RedisYRoomStorage(BaseYRoomStorage):
    """A YRoom storage that uses Redis as main storage, without
    persistent storage.

    Args:
        room_name: The name of the room.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.redis_key = f"document:{self.room_name}"
        self.redis = self.make_redis()

    def make_redis(self):
        """Makes a Redis client.

        Defaults to a local client"""

        return redis.Redis(host="localhost", port=6379, db=0)

    async def get_document(self) -> Y.YDoc:
        snapshot = await self.redis.get(self.redis_key)

        if not snapshot:
            snapshot = await self.get_snapshot_from_database()

        document = Y.YDoc()

        if snapshot:
            Y.apply_update(document, snapshot)

        return document

    async def get_snapshot_from_database(self) -> Optional[bytes]:
        """Gets the document from the database. Override this method to
        implement a persistent storage.

        Defaults to None.

        Returns:
            (Optional) The document with the latest changes.
        """
        return None

    async def update_document(self, update: bytes):
        await self.redis.watch(self.redis_key)

        try:
            current_document = await self.get_document()
            updated_snapshot = self._apply_update_to_snapshot(current_document, update)

            async with self.redis.pipeline() as pipe:
                while True:
                    try:
                        pipe.multi()
                        pipe.set(self.redis_key, updated_snapshot)

                        await pipe.execute()

                        break
                    except redis.WatchError:
                        current_snapshot = await self.get_document()
                        updated_snapshot = self._apply_update_to_snapshot(
                            current_snapshot,
                            update,
                        )

                        continue
        finally:
            await self.redis.unwatch()

        await self.debounced_persist_document()

    async def close(self):
        await self.persist_document()
        await self.redis.close()
