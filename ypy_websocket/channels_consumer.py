from __future__ import annotations

from logging import getLogger
from typing import TypedDict

from channels.generic.websocket import AsyncWebsocketConsumer
import y_py as Y
from .websocket import Websocket
from .yutils import YMessageType, process_sync_message, sync

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


class ChannelsConsumer(AsyncWebsocketConsumer):
    """A working consumer for [Django Channels](https://github.com/django/channels).

    Can be used out of the box with something like
    ```py
    path("ws/<str:room>", ChannelsConsumer.as_asgi()),
    ```
    or subclassed to customize the behavior.

    In particular,
    - Override `make_room_name` to customize the room name
    - Override `make_ydoc` to initialize the YDoc
      (useful to initialize it with data from your database, or to add observers to it).
    """

    def __init__(self):
        super().__init__()
        self.room_name = None
        self.ydoc = None
        self._websocket_shim = None

    def make_room_name(self) -> str:
        """Make the room name for a new channel.

        Override to customize the room name when a channel is created.

        Returns:
            The room name for a new channel. Defaults to the room name from the URL route.
        """
        return self.scope["url_route"]["kwargs"]["room"]

    async def make_ydoc(self) -> Y.YDoc:
        """Make the YDoc for a new channel.

        Override to customize the YDoc when a channel is created
        (useful to initialize it with data from your database, or to add observers to it).

        Returns:
            The YDoc for a new channel. Defaults to a new empty YDoc.
        """
        return Y.YDoc()

    def _make_websocket_shim(self, path: str) -> _WebsocketShim:
        return _WebsocketShim(path, self.group_send_message)

    async def connect(self) -> None:
        self.room_name = self.make_room_name()
        self.ydoc = await self.make_ydoc()
        self._websocket_shim = self._make_websocket_shim(self.scope["path"])

        await self.channel_layer.group_add(self.room_name, self.channel_name)
        await self.accept()

        await sync(self.ydoc, self._websocket_shim, logger)

    async def disconnect(self, code) -> None:
        await self.channel_layer.group_discard(self.room_name, self.channel_name)

    async def receive(self, text_data=None, bytes_data=None):
        if bytes_data is None:
            return
        await self.group_send_message(bytes_data)
        if bytes_data[0] != YMessageType.SYNC:
            return
        await process_sync_message(
            bytes_data[1:], self.ydoc, self._websocket_shim, logger
        )

    async def send_message(
        self, message_wrapper: TypedDict("MessageWrapper", {"message": bytes})
    ) -> None:
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
