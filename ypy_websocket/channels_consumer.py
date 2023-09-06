from __future__ import annotations

from logging import getLogger

from channels.generic.websocket import AsyncWebsocketConsumer
import y_py as Y
from .websocket import Websocket
from .yutils import YMessageType, process_sync_message, sync

logger = getLogger(__name__)


class _WebsocketShim(Websocket):
    def __init__(self, path, room_name, channel_layer) -> None:
        self._path = path
        self._room_name = room_name
        self._channel_layer = channel_layer

    @property
    def path(self) -> str:
        return self._path

    def __aiter__(self):
        raise NotImplementedError()

    async def __anext__(self) -> bytes:
        raise NotImplementedError()

    async def send(self, message: bytes) -> None:
        await self._channel_layer.group_send(
            self._room_name, {"type": "_send_message", "message": message}
        )

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
        return _WebsocketShim(path, self.room_name, self.channel_layer)

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
        if bytes_data is None or bytes_data[0] != YMessageType.SYNC:
            return
        await process_sync_message(
            bytes_data[1:], self.ydoc, self._websocket_shim, logger
        )

    async def _send_message(self, message_wrapper) -> None:
        await self.send(bytes_data=message_wrapper["message"])
