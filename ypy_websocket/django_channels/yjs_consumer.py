from __future__ import annotations

from logging import getLogger
from typing import TypedDict

import y_py as Y
from channels.generic.websocket import AsyncWebsocketConsumer

from ypy_websocket.django_channels.yroom_storage import BaseYRoomStorage

from ..websocket import Websocket
from ..yutils import (
    EMPTY_UPDATE,
    YMessageType,
    YSyncMessageType,
    process_sync_message,
    read_message,
    sync,
)

logger = getLogger(__name__)


class _WebsocketShim(Websocket):
    def __init__(self, path, send_func) -> None:
        self._path = path
        self._send_func = send_func

    @property
    def path(self) -> str:
        return self._path

    def __aiter__(self):
        raise NotImplementedError()

    async def __anext__(self) -> bytes:
        raise NotImplementedError()

    async def send(self, message: bytes) -> None:
        await self._send_func(message)

    async def recv(self) -> bytes:
        raise NotImplementedError()


class YjsConsumer(AsyncWebsocketConsumer):
    """A working consumer for [Django Channels](https://github.com/django/channels).

    This consumer can be used out of the box simply by adding:
    ```py
    path("ws/<str:room>", YjsConsumer.as_asgi())
    ```
    to your `urls.py` file. In practice, once you
    [set up Channels](https://channels.readthedocs.io/en/1.x/getting-started.html),
    you might have something like:
    ```py
    # urls.py
    from django.urls import path
    from backend.consumer import DocConsumer, UpdateConsumer

    urlpatterns = [
        path("ws/<str:room>", YjsConsumer.as_asgi()),
    ]

    # asgi.py
    import os
    from channels.routing import ProtocolTypeRouter, URLRouter
    from urls import urlpatterns

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")

    application = ProtocolTypeRouter({
        "websocket": URLRouter(urlpatterns_ws),
    })
    ```

    Additionally, the consumer can be subclassed to customize its behavior.

    In particular,

    - Override `make_room_name` to customize the room name.
    - Override `make_room_storage` to initialize the room storage. Create your own storage class
      by subclassing `BaseYRoomStorage` and implementing the methods.
    - Override `connect` to do custom validation (like auth) on connect,
      but be sure to call `await super().connect()` in the end.
    - Call `group_send_message` to send a message to an entire group/room.
    - Call `send_message` to send a message to a single client, although this is not recommended.
    - Call `propagate_document_update_from_external` to send a message to all connected clients from an external source (like a Celery job).

    A full example of a custom consumer showcasing all of these options is:

    ```py
    import y_py as Y
    from asgiref.sync import async_to_sync
    from channels.layers import get_channel_layer
    from ypy_websocket.django_channels_consumer import YjsConsumer
    from ypy_websocket.yutils import create_update_message


    class DocConsumer(YjsConsumer):
        def make_room_storage(self) -> BaseYRoomStorage:
            # Modify the room storage here

            return RedisYRoomStorage(self.room_name)

        def make_room_name(self) -> str:
            # Modify the room name here

            return self.scope["url_route"]["kwargs"]["room"]

        async def connect(self):
            user = self.scope["user"]

            if user is None or user.is_anonymous:
                await self.close()
                return

            await super().connect()

        async def propagate_document_update(self, update_wrapper):
            update = update_wrapper["update"]

            await self.send(create_update_message(update))


    async def propagate_document_update_from_external(room_name, update):
        channel_layer = get_channel_layer()

        await channel_layer.group_send(
            room_name,
            {"type": "propagate_document_update", "update": update},
        )
    ```
    """

    def __init__(self):
        super().__init__()
        self.room_name: str | None = None
        self.ydoc: Y.YDoc | None = None
        self.room_storage: BaseYRoomStorage | None = None
        self._websocket_shim: _WebsocketShim | None = None

    def make_room_storage(self) -> BaseYRoomStorage | None:
        """Make the room storage for a new channel to persist the YDoc after
        the room has no more consumers.

        Defaults to not using any (just broadcast updates between consumers).

        Example:
            self.room_storage = RedisYRoomStorage(self.room_name)
        """
        return None

    def make_room_name(self) -> str:
        """Make the room name for a new channel.

        Override to customize the room name when a channel is created.

        Returns:
            The room name for a new channel. Defaults to the room name from the URL route.
        """
        return self.scope["url_route"]["kwargs"]["room"]

    async def _make_ydoc(self) -> Y.YDoc:
        if self.room_storage:
            return await self.room_storage.get_document()

        return Y.YDoc()

    def _make_websocket_shim(self, path: str) -> _WebsocketShim:
        return _WebsocketShim(path, self.group_send_message)

    async def connect(self) -> None:
        self.room_name = self.make_room_name()
        self.room_storage = self.make_room_storage()

        self.ydoc = await self._make_ydoc()
        self._websocket_shim = self._make_websocket_shim(self.scope["path"])

        await self.channel_layer.group_add(self.room_name, self.channel_name)
        await self.accept()

        await sync(self.ydoc, self._websocket_shim, logger)

    async def disconnect(self, code) -> None:
        if self.room_storage:
            await self.room_storage.close()

        if not self.room_name:
            return

        await self.channel_layer.group_discard(self.room_name, self.channel_name)

    async def receive(self, text_data=None, bytes_data=None):
        if bytes_data is None:
            return

        await self.group_send_message(bytes_data)

        if bytes_data[0] != YMessageType.SYNC:
            return

        # If it's an update message, apply it to the storage document
        if self.room_storage and bytes_data[1] == YSyncMessageType.SYNC_UPDATE:
            update = read_message(bytes_data[2:])

            if update != EMPTY_UPDATE:
                await self.room_storage.update_document(update)

            return

        await process_sync_message(bytes_data[1:], self.ydoc, self._websocket_shim, logger)

    class WrappedMessage(TypedDict):
        """A wrapped message to send to the client."""

        message: bytes

    async def send_message(self, message_wrapper: WrappedMessage) -> None:
        """Send a message to the client.

        Arguments:
            message_wrapper: The message to send, wrapped.
        """
        await self.send(bytes_data=message_wrapper["message"])

    async def group_send_message(self, message: bytes) -> None:
        """Send a message to the group.

        Arguments:
            message: The message to send.
        """
        await self.channel_layer.group_send(
            self.room_name, {"type": "send_message", "message": message}
        )
