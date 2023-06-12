The WebSocket object passed to `WebsocketProvider` and `WebsocketServer.serve` must implement the following API, defined as a [protocol class](../../reference/WebSocket):

```py
class WebSocket:

    @property
    def path(self) -> str:
        # can be e.g. the URL path
        # or a room identifier
        return "my-roomname"

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        # async iterator for receiving messages
        # until the connection is closed
        try:
            message = await self.recv()
        except Exception:
            raise StopAsyncIteration()

        return message

    async def send(self, message: bytes):
        # send message
        pass

    async def recv(self) -> bytes:
        # receive message
        return b""
```
