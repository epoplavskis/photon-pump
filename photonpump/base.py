import asyncio
import struct
from typing import Dict, Sequence
import uuid
from uuid import UUID

from .messages import (
    Operation, Header, TcpCommand, Ping, NewEvent, WriteEvents, NewEventData,
    ExpectedVersion, ReadEvent, StreamDirection, ReadStreamEvents,
    IterStreamEvents, HeartbeatResponse
)

__version__ = '0.1.0'

HEADER_LENGTH = 1 + 1 + 16
SIZE_UINT_32 = 4

#: 1 byte command + 1 byte auth + UUID correlation length
FLAGS_NONE = 0x00


class Event(list):

    def __call__(self, *args, **kwargs):
        for f in self:
            f(*args, **kwargs)

    def __repr__(self):
        return "Event(%)" % list.__repr__(self)


class OutChannel:
    """Wraps an asyncio StreamWriter with an operations queue.

    Args:
        writer: an asyncio StreamWriter.
        pending: a dict for storing pending operations.
    """

    def __init__(
            self,
            writer: asyncio.StreamWriter,
            pending: Dict[UUID, Operation], loop):
        self.queue = asyncio.Queue(maxsize=100, loop=loop)
        self.pending_operations = pending
        self.writer = writer
        self.running = True
        self.write_loop = asyncio.ensure_future(self._process())

    async def enqueue(self, message: Operation):
        """Enqueue an operation.

        The operation will be added to the `pending` dict, and
            pushed onto the queue for sending.

        Args:
            message: The operation to send.
        """
        print("Enqueuing %s" % (message,))
        self.pending_operations[message.correlation_id] = message
        await self.queue.put(message)

    async def _process(self):
        while self.running:
            next = await self.queue.get()
            print(next)
            next.send(self.writer)

    def close(self):
        """Close the underlying StreamWriter and cancel pending Operations."""
        self.writer.close()
        self.running = False
        self.write_loop.cancel()


class InChannel:
    """
    Reads an asyncio.StreamReader and invokes pending operations
    with their responses

    Args:
        reader: an asyncio StreamReader.
        writer: an OutChannel used for retrying or sending new replies.
        pending: a dict for storing pending operations.
    """

    _length = struct.Struct('<I')
    _head = struct.Struct('>BBQQ')

    def __init__(
            self,
            reader:
            asyncio.StreamReader,
            writer: OutChannel,
            pending: Dict[UUID, Operation]):
        self.pending = pending
        self.reader = reader
        self.writer = writer
        self.read_loop = asyncio.ensure_future(self.read_responses())

    async def _read_header(self):
        """Read a message header from the StreamReader."""
        next_msg_len = await self.reader.read(SIZE_UINT_32)
        next_header = await self.reader.read(HEADER_LENGTH)
        (cmd, flags, a, b) = self._head.unpack(next_header)
        (size,) = self._length.unpack(next_msg_len)
        id = uuid.UUID(int=(a << 64 | b))
        return Header(size, cmd, flags, id)

    async def read_message(self):
        header = await self._read_header()
        bytes_remaining = header.size - HEADER_LENGTH
        next_chunk = await self.reader.read(bytes_remaining)
        bytes_read = len(next_chunk)
        if bytes_read == bytes_remaining:
            return header, next_chunk
        else:
            body = bytearray(next_chunk)
            while bytes_remaining > 0:
                bytes_remaining -= bytes_read
                next_chunk = await self.reader.read(bytes_remaining)
                bytes_read = len(next_chunk)
                body.extend(next_chunk)
            return header, body

    async def read_responses(self):
        """
        Loop forever reading messages and invoking the operation
        that caused them
        """
        while True:
            header, data = await self.read_message()

            if header.cmd == TcpCommand.HeartbeatRequest:
                await self.writer.enqueue(HeartbeatResponse(id))
                continue

            operation = self.pending[header.correlation_id]
            await operation.handle_response(header, data, self.writer)
            del self.pending[header.correlation_id]

    def close(self):
        self.read_loop.cancel()


class Connection:
    """Top level object for interacting with Eventstore.

    The connection is the entry point to working with Photon Pump.
    It exposes high level methods that wrap the
    :class:`~photonpump.messages.Operation` types from photonpump.messages.
    """

    def __init__(self, host='127.0.0.1', port=1113, loop=None):
        self.connected = Event()
        self.disconnected = Event()
        self.host = host
        self.port = port
        self.loop = loop
        self.operations = {}

    async def connect(self):
        reader, writer = await asyncio.open_connection(
                self.host,
                self.port,
                loop=self.loop)
        self.writer = OutChannel(
                writer,
                self.operations,
                loop=self.loop)
        self.reader = InChannel(
                reader,
                self.writer,
                self.operations
                )
        self.connected()

    def close(self):
        self.writer.close()
        self.reader.close()
        self.disconnected()

    async def ping(self, correlation_id: UUID=None):
        correlation_id = correlation_id
        cmd = Ping(correlation_id=correlation_id, loop=self.loop)
        await self.writer.enqueue(cmd)
        return await cmd.future

    async def publish_event(
            self,
            type,
            stream,
            body=None,
            id=uuid.uuid4(),
            metadata=None,
            expected_version=-2,
            require_master=False):
        event = NewEvent(type, id, body, metadata)
        cmd = WriteEvents(
            stream, [event], expected_version=expected_version,
            require_master=require_master, loop=self.loop
        )
        await self.writer.enqueue(cmd)
        return await cmd.future

    async def publish(
            self,
            stream: str,
            events: Sequence[NewEventData],
            expected_version=ExpectedVersion.Any,
            require_master=False):
        cmd = WriteEvents(
            stream, events, expected_version=expected_version,
            require_master=require_master, loop=self.loop
        )
        await self.writer.enqueue(cmd)
        return await cmd.future

    async def get_event(
            self,
            stream: str,
            resolve_links=True,
            require_master=False,
            correlation_id: UUID=None):
        correlation_id = correlation_id
        cmd = ReadEvent(stream, resolve_links, require_master, loop=self.loop)
        await self.writer.enqueue(cmd)
        return await cmd.future

    async def get(
            self,
            stream: str,
            direction: StreamDirection=StreamDirection.Forward,
            from_event: int=0,
            max_count: int=100,
            resolve_links: bool=True,
            require_master: bool=False,
            correlation_id: UUID=None):
        correlation_id = correlation_id
        cmd = ReadStreamEvents(
            stream, from_event, max_count, resolve_links, require_master,
            loop=self.loop
        )
        await self.writer.enqueue(cmd)
        return await cmd.future

    async def iter(
            self,
            stream: str,
            direction: StreamDirection=StreamDirection.Forward,
            from_event: int=0,
            batch_size: int=100,
            resolve_links: bool=True,
            require_master: bool=False,
            correlation_id: UUID=None):
        correlation_id = correlation_id
        cmd = IterStreamEvents(
            stream, from_event, batch_size, resolve_links, require_master,
            loop=self.loop
        )
        await self.writer.enqueue(cmd)
        async for e in cmd.iterator:
            yield e


class ConnectionContextManager:

    def __init__(self, host='127.0.0.1', port=1113, loop=None):
        self.conn = Connection(host=host, port=port, loop=loop)

    async def __aenter__(self):
        await self.conn.connect()
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        self.conn.close()


def connect(*args, **kwargs):
    return ConnectionContextManager(*args, **kwargs)
