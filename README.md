# Ypy-websocket

Ypy-websocket is an async WebSocket connector for [Ypy](https://github.com/y-crdt/ypy).

[![Build Status](https://github.com/y-crdt/ypy-websocket/workflows/CI/badge.svg)](https://github.com/y-crdt/ypy-websocket/actions)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

---

**Documentation**: <a href="https://davidbrochart.github.io/ypy-websocket" target="_blank">https://davidbrochart.github.io/ypy-websocket</a>

**Source Code**: <a href="https://github.com/y-crdt/ypy-websocket" target="_blank">https://github.com/y-crdt/ypy-websocket</a>

---

Ypy-websocket is a Python library for building WebSocket servers and clients that connect and synchronize shared documents.
It can be used to create collaborative web applications.

The following diagram illustrates a typical architecture. The goal is to share a document among several clients.

Each client has an instance of a [YDoc](https://ypy.readthedocs.io/en/latest/autoapi/y_py/index.html#y_py.YDoc), representing their view of a document. A shared document also lives in a [room](./reference/Room.md) on the server side. Conceptually, a room can be seen as the place where clients collaborate on a document. The WebSocket to which a client connects points to the corresponding room through the endpoint path. In the example below, clients A and B connect to a WebSocket at path `room-1`, and thus both clients find themselves in a room called `room-1`. All the `YDoc` synchronization logic is taken care of by the [WebsocketProvider](./reference/WebSocket_provider.md).

Each update to a shared document can be persisted to disk using a [store](./reference/Store.md), which can be a file or a database.
```mermaid
flowchart TD
    classDef room1 fill:#f96
    classDef room2 fill:#bbf
    A[Client A<br>room-1]:::room1 <-->|WebSocket<br>Provider| server(WebSocket Server)
    B[Client B<br>room-1]:::room1 <-->|WebSocket<br>Provider| server
    C[Client C<br>room-2]:::room2 <-->|WebSocket<br>Provider| server
    D[Client D<br>room-2]:::room2 <-->|WebSocket<br>Provider| server
    server <--> room1((room-1<br>clients: A, B)):::room1
    server <--> room2((room-2<br>clients: C, D)):::room2
    A <-..-> room1
    B <-..-> room1
    C <-..-> room2
    D <-..-> room2
    room1 ---> store1[(Store)]
    room2 ---> store2[(Store)]
```
