import array
import binascii
import json
import logging
import math
import struct
from asyncio import Future, Queue
from collections import namedtuple
from enum import IntEnum
from typing import Any, Dict, NamedTuple, Sequence, Union
from uuid import UUID, uuid4

from google.protobuf import text_format

from . import exceptions, messages_pb2

HEADER_LENGTH = 1 + 1 + 16
SIZE_UINT_32 = 4
_LENGTH = struct.Struct('<I')
_HEAD = struct.Struct('<BBIIII')


def make_enum(descriptor):
    vals = [(x.name, x.number) for x in descriptor.values]

    return IntEnum(descriptor.name, vals)


class TcpCommand(IntEnum):

    HeartbeatRequest = 0x01
    HeartbeatResponse = 0x02

    Ping = 0x03
    Pong = 0x04

    WriteEvents = 0x82
    WriteEventsCompleted = 0x83

    Read = 0xB0
    ReadEventCompleted = 0xB1
    ReadStreamEventsForward = 0xB2
    ReadStreamEventsForwardCompleted = 0xB3
    ReadStreamEventsBackward = 0xB4
    ReadStreamEventsBackwardCompleted = 0xB5
    ReadAllEventsForward = 0xB6
    ReadAllEventsForwardCompleted = 0xB7
    ReadAllEventsBackward = 0xB8
    ReadAllEventsBackwardCompleted = 0xB9

    SubscribeToStream = 0xC0
    SubscriptionConfirmation = 0xC1
    StreamEventAppeared = 0xC2
    SubscriptionDropped = 0xC4

    ConnectToPersistentSubscription = 0xC5
    PersistentSubscriptionConfirmation = 0xC6
    PersistentSubscriptionStreamEventAppeared = 0xC7
    CreatePersistentSubscription = 0xC8
    CreatePersistentSubscriptionCompleted = 0xC9
    DeletePersistentSubscription = 0xCA
    DeletePersistentSubscriptionCompleted = 0xCB
    PersistentSubscriptionAckEvents = 0xCC
    PersistentSubscriptionNakEvents = 0xCD
    UpdatePersistentSubscription = 0xCE
    UpdatePersistentSubscriptionCompleted = 0xCF

    BadRequest = 0xf0


class StreamDirection(IntEnum):
    Forward = 0
    Backward = 1


class ContentType(IntEnum):
    Json = 0x01
    Binary = 0x00


class OperationFlags(IntEnum):
    Empty = 0x00
    Authenticated = 0x01


class Credential:

    def __init__(self, username, password):
        self.username = username
        self.password = password

        username_bytes = username.encode('UTF-8')
        password_bytes = password.encode('UTF-8')

        self.length = 2 + len(username_bytes) + len(password_bytes)
        self.bytes = bytearray()

        self.bytes.extend(len(username_bytes).to_bytes(1, byteorder='big'))
        self.bytes.extend(username_bytes)
        self.bytes.extend(len(password_bytes).to_bytes(1, byteorder='big'))
        self.bytes.extend(password_bytes)

    @classmethod
    def from_bytes(cls, data):
        """
        I am so sorry.
        """
        len_username = int.from_bytes(data[0:2], byteorder='big')
        offset_username = 2 + len_username
        username = data[2:offset_username].decode('UTF-8')
        offset_password = 2 + offset_username
        len_password = int.from_bytes(
            data[offset_username:offset_password], byteorder='big'
        )
        password = data[offset_password:offset_password + len_password
                       ].decode('UTF-8')

        return cls(username, password)


class InboundMessage:

    def __init__(
            self, conversation_id: UUID, command: TcpCommand, payload: Any,
            length: int
    ) -> None:
        self.conversation_id = conversation_id
        self.command = command
        self.payload = payload
        self.length = length
        self.data_length = length - HEADER_LENGTH


class OutboundMessage:

    def __init__(
            self,
            conversation_id: UUID,
            command: TcpCommand,
            payload: Any,
            creds: Credential = None
    ) -> None:
        self.conversation = conversation_id
        self.command = command
        self.payload = payload
        self.creds = creds


class MessageReader:

    MESSAGE_MIN_SIZE = SIZE_UINT_32 + HEADER_LENGTH

    def __init__(self, on_message_received):
        self.on_message_received = on_message_received
        self.header_bytes = array.array('B', [0] * (self.MESSAGE_MIN_SIZE))
        self.header_bytes_required = (self.MESSAGE_MIN_SIZE)
        self.length = 0
        self.message_offset = 0
        self.conversation_id = None

    def process(self, chunk: bytes):
        chunk_offset = 0
        chunk_len = len(chunk)

        while self.header_bytes_required and chunk_offset < chunk_len:
            self.header_bytes[self.MESSAGE_MIN_SIZE - self.header_bytes_required
                             ] = chunk[chunk_offset]
            chunk_offset += 1
            self.message_offset += 1
            self.header_bytes_required -= 1

            if not self.header_bytes_required:
                (self.length, self.cmd, self.flags) = struct.unpack(
                    '<IBB', self.header_bytes[0:6]
                )

                self.conversation_id = UUID(bytes_le=(self.header_bytes[6:22].tobytes()))

        if self.length == (self.message_offset - SIZE_UINT_32):
            self.on_message_received(
                InboundMessage(
                    self.conversation_id, self.cmd, bytearray(), self.length
                )
            )

        if chunk_len == chunk_offset:
            return


class HeartbeatConversation:

    def __init__(self, conversation_id: UUID):
        self.conversation = conversation


class ExpectedVersion(IntEnum):
    """Static values for concurrency control

    Attributes:
        Any: No concurrency control.
        StreamMustNotExist: The request should fail if the stream
          already exists.
        StreamMustBeEmpty: The request should fail if the stream
          does not exist, or if the stream already contains events.
        StreamMustExist: The request should fail if the stream
          does not exist.
    """

    Any = -2
    StreamMustNotExist = -1
    StreamMustBeEmpty = 0
    StreamMustExist = -4


JsonDict = Dict[str, Any]
Header = namedtuple(
    'photonpump_result_header',
    ['size', 'cmd', 'flags', 'correlation_id', 'username', 'password']
)


def parse_header(length: bytearray, data: bytearray) -> Header:
    (size, ) = _LENGTH.unpack(length)

    return Header(size, data[0], data[1], UUID(bytes_le=data[2:18]), None, None)


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0

    return "%.1f%s%s" % (num, 'Yi', suffix)


def print_header(header):
    return "%s (%s) of %s flags=%d" % (
        TcpCommand(header.cmd).name, header.correlation_id,
        sizeof_fmt(header.size), header.flags
    )


Header.__repr__ = print_header
Header.__new__.__defaults__ = (None, None)

NewEventData = namedtuple(
    'photonpump_event', ['id', 'type', 'data', 'metadata']
)

EventRecord = namedtuple(
    'photonpump_eventrecord',
    ['stream', 'id', 'event_number', 'type', 'data', 'metadata', 'created']
)


def _json(self):
    return json.loads(self.data.decode('UTF-8'))


EventRecord.json = _json


class StreamingIterator:

    def __init__(self, size):
        self.items = Queue(maxsize=size)
        self.finished = False
        self.fut = None

    async def __aiter__(self):
        return self

    async def enqueue_items(self, items):
        for item in items:
            await self.items.put(item)

    async def enqueue(self, item):
        await self.items.put(item)

    async def __anext__(self):
        if self.finished and self.items.empty():
            raise StopAsyncIteration()
        try:
            next = await self.items.get()
        except Exception as e:
            raise StopAsyncIteration()

        if isinstance(next, StopIteration):
            raise StopAsyncIteration()

        if isinstance(next, Exception):
            raise next

        return next

    async def athrow(self, e):
        await self.items.put(e)

    async def asend(self, m):
        await self.items.put(m)

    def cancel(self):
        self.finished = True
        self.asend(StopIteration())


class Event:

    def __init__(self, event: EventRecord, link: EventRecord) -> None:
        self.event = event
        self.link = link

    @property
    def original_event(self) -> EventRecord:
        return self.link or self

    @property
    def original_event_id(self) -> UUID:
        return self.original_event.id


def dump(*chunks: bytearray):
    data = bytearray()

    for chunk in chunks:
        data.extend(chunk)

    length = len(data)
    rows = length / 16

    dump = []

    for i in range(0, math.ceil(rows)):
        offset = i * 16
        row = data[offset:offset + 16]
        hex = "{0: <47}".format(" ".join("{:02x}".format(x) for x in row))
        dump.append("%06d: %s | %a" % (offset, hex, bytes(row)))
    logging.debug("\n".join(dump))


def _make_event(record: messages_pb2.ResolvedEvent):

    link = EventRecord(
        record.link.event_stream_id,
        UUID(bytes_le=record.link.event_id),
        record.link.event_number,
        record.link.event_type,
        record.link.data,
        record.link.metadata,
        record.link.created_epoch
    ) if record.link is not None else None

    event = EventRecord(
        record.event.event_stream_id,
        UUID(bytes_le=record.event.event_id),
        record.event.event_number,
        record.event.event_type,
        record.event.data,
        record.event.metadata,
        record.event.created_epoch
    )

    return Event(event, link)


class Operation:
    """The base class for requests to Eventstore.

    Implementors have two responsibilities: they must serialize a byte-stream
    request in the :meth:`~photonpump.messages.Operation.send` method, and
    they must deserialize and handle the response in the
    :meth:`~photonpump.messages.Operation.handle_response` method.
    """

    one_way = False

    def __init__(self, credential: Credential = None):
        self.credential = credential

        if credential is None:
            self.flags = OperationFlags.Empty
        else:
            self.flags = OperationFlags.Authenticated

    def send(self, writer):
        """
        Write the byte-stream of this request to an instance of StreamWriter
        """
        header = self.make_header()
        dump(header[4:], self.data)
        writer.write(header)
        writer.write(self.data)

    def make_header(self):
        """Build the byte-array representing the operation's header."""
        buf = bytearray()
        data_length = len(self.data)
        authn_length = self.credential.length if self.credential is not None else 0
        buf.extend(
            struct.pack(
                '<IBB', HEADER_LENGTH + authn_length + data_length,
                self.command, self.flags
            )
        )
        buf.extend(self.correlation_id.bytes_le)

        if self.flags == OperationFlags.Authenticated:
            buf.extend(self.credential.bytes)

        return buf

    async def handle_response(self, header, payload, writer):
        """Handle the response from Eventstore.

        Implementors can choose whether to return a single result,

        return an async generator, or send a new Operation to the
        :class:`photonpump.Connection`.
        """
        self.is_complete = True

    def __repr__(self):
        return "Operation %s (%s)" % (
            self.__class__.__name__, self.correlation_id
        )


Pong = namedtuple('photonpump_result_Pong', ['correlation_id'])


class Ping(Operation):
    """Command class for server pings.

    Args:
        correlation_id (optional): A unique identifer for this command.
    """

    def __init__(self, correlation_id: UUID = None, credential=None, loop=None):
        self.flags = OperationFlags.Empty
        self.command = TcpCommand.Ping
        self.future = Future(loop=loop)
        self.correlation_id = correlation_id or uuid4()
        self.data = bytearray()
        super().__init__(credential)

    def cancel(self):
        self.future.cancel()

    async def handle_response(self, header, payload, writer) -> Pong:
        self.is_complete = True
        self.future.set_result(Pong(header.correlation_id))


def NewEvent(
        type: str,
        id: UUID = uuid4(),
        data: JsonDict = None,
        metadata: JsonDict = None
) -> NewEventData:
    """Build the data structure for a new event.

    Args:
        type: An event type.
        id: The uuid identifier for the event.
        data: A dict containing data for the event. These data
            must be json serializable.
        metadata: A dict containing metadata about the event.
            These must be json serializable.
    """

    return NewEventData(id, type, data, metadata)


class WriteEvents(Operation):
    """Command class for writing a sequence of events to a single
        stream.

    Args:
        stream: The name of the stream to write to.
        events: A sequence of events to write.
        expected_version (optional): The expected version of the
            target stream used for concurrency control.
        required_master (optional): True if this command must be
            sent direct to the master node, otherwise False.
        correlation_id (optional): A unique identifer for this
            command.

    """

    def __init__(
            self,
            stream: str,
            events: Sequence[NewEventData],
            expected_version: Union[ExpectedVersion, int] = ExpectedVersion.Any,
            require_master: bool = False,
            correlation_id: UUID = None,
            credential=None,
            loop=None
    ):
        self.correlation_id = correlation_id or uuid4()
        self.future = Future(loop=loop)
        self.flags = OperationFlags.Empty
        self.command = TcpCommand.WriteEvents

        msg = messages_pb2.WriteEvents()
        msg.event_stream_id = stream
        msg.require_master = require_master
        msg.expected_version = expected_version

        for event in events:
            e = msg.events.add()
            e.event_id = event.id.bytes
            e.event_type = event.type

            if isinstance(event.data, str):
                e.data_content_type = ContentType.Json
                e.data = event.data.encode('UTF-8')
            elif event.data:
                e.data_content_type = ContentType.Json
                e.data = json.dumps(event.data).encode('UTF-8')
            else:
                e.data_content_type = ContentType.Binary
                e.data = bytes()

            if event.metadata:
                e.metadata_content_type = ContentType.Json
                e.metadata = json.dumps(event.metadata).encode('UTF-8')
            else:
                e.metadata_content_type = ContentType.Binary
                e.metadata = bytes()

        self.data = msg.SerializeToString()
        super().__init__(credential)

    async def handle_response(self, header, payload, writer):
        result = messages_pb2.WriteEventsCompleted()
        result.ParseFromString(payload)
        self.is_complete = True
        self.future.set_result(result)

    def cancel(self):
        self.future.cancel()


class ReadEvent(Operation):
    """Command class for reading a single event.

    Args:
        stream: The name of the stream containing the event.
        event_number: The sequence number of the event to read.
        resolve_links (optional): True if eventstore should
            automatically resolve Link Events, otherwise False.
        required_master (optional): True if this command must be
            sent direct to the master node, otherwise False.
        correlation_id (optional): A unique identifer for this
            command.

    """

    def __init__(
            self,
            stream: str,
            event_number: int,
            resolve_links: bool = True,
            require_master: bool = False,
            correlation_id: UUID = None,
            credentials=None,
            loop=None
    ):

        self.correlation_id = correlation_id or uuid4()
        self.future = Future(loop=loop)
        self.flags = OperationFlags.Empty
        self.command = TcpCommand.Read
        self.stream = stream

        msg = messages_pb2.ReadEvent()
        msg.event_number = event_number
        msg.event_stream_id = stream
        msg.require_master = require_master
        msg.resolve_link_tos = resolve_links

        self.data = msg.SerializeToString()
        super().__init__(credentials)

    async def handle_response(self, header, payload, writer):
        result = messages_pb2.ReadEventCompleted()
        result.ParseFromString(payload)

        self.is_complete = True

        if result.result == ReadEventResult.Success:
            self.future.set_result(_make_event(result.event))
        elif result.result == ReadEventResult.NoStream:
            msg = "The stream '" + self.stream + "' was not found"
            exn = exceptions.StreamNotFoundException(msg, self.stream)
            self.future.set_exception(exn)

    def cancel(self):
        self.future.cancel()


ReadEventResult = make_enum(messages_pb2._READEVENTCOMPLETED_READEVENTRESULT)

ReadStreamResult = make_enum(
    messages_pb2._READSTREAMEVENTSCOMPLETED_READSTREAMRESULT
)


class ReadStreamEvents(Operation):
    """Command class for reading events from a stream.

    Args:
        stream: The name of the stream containing the event.
        event_number: The sequence number of the event to read.
        resolve_links (optional): True if eventstore should
            automatically resolve Link Events, otherwise False.
        required_master (optional): True if this command must be
            sent direct to the master node, otherwise False.
        correlation_id (optional): A unique identifer for this
            command.

    """

    def __init__(
            self,
            stream: str,
            from_event: int,
            max_count: int = 100,
            resolve_links: bool = True,
            require_master: bool = False,
            direction: StreamDirection = StreamDirection.Forward,
            credentials=None,
            correlation_id: UUID = None,
            loop=None
    ):

        self.correlation_id = correlation_id or uuid4()
        self.future = Future(loop=loop)
        self.flags = OperationFlags.Empty
        self.stream = stream

        if direction == StreamDirection.Forward:
            self.command = TcpCommand.ReadStreamEventsForward
        else:
            self.command = TcpCommand.ReadStreamEventsBackward

        msg = messages_pb2.ReadStreamEvents()
        msg.event_stream_id = stream
        msg.from_event_number = from_event
        msg.max_count = max_count
        msg.require_master = require_master
        msg.resolve_link_tos = resolve_links

        self.data = msg.SerializeToString()
        super().__init__(credentials)

    async def handle_response(self, header, payload, writer):
        result = messages_pb2.ReadStreamEventsCompleted()
        self.is_complete = True
        result.ParseFromString(payload)

        if result.result == ReadStreamResult.Success:
            self.future.set_result([_make_event(x) for x in result.events])
        elif result.result == ReadEventResult.NoStream:
            msg = "The stream '" + self.stream + "' was not found"
            exn = exceptions.StreamNotFoundException(msg, self.stream)
            self.future.set_exception(exn)

    def cancel(self):
        self.future.cancel()


class IterStreamEvents(Operation):
    """Command class for iterating events from a stream.

    Args:
        stream: The name of the stream containing the event.
        resolve_links (optional): True if eventstore should
            automatically resolve Link Events, otherwise False.
        required_master (optional): True if this command must be
            sent direct to the master node, otherwise False.
        correlation_id (optional): A unique identifer for this
            command.

    """

    def __init__(
            self,
            stream: str,
            from_event: int,
            batch_size: int = 100,
            resolve_links: bool = True,
            require_master: bool = False,
            direction: StreamDirection = StreamDirection.Forward,
            credentials=None,
            correlation_id: UUID = None,
            iterator: StreamingIterator = None,
            loop=None
    ):

        self.correlation_id = correlation_id or uuid4()
        self.batch_size = batch_size
        self.stream = stream
        self.iterator = iterator or StreamingIterator(batch_size * 2)
        self.resolve_links = resolve_links
        self.require_master = require_master
        self.direction = direction

        if direction == StreamDirection.Forward:
            self.command = TcpCommand.ReadStreamEventsForward
        else:
            self.command = TcpCommand.ReadStreamEventsBackward

        msg = messages_pb2.ReadStreamEvents()
        msg.event_stream_id = stream
        msg.from_event_number = from_event
        msg.max_count = batch_size
        msg.require_master = require_master
        msg.resolve_link_tos = resolve_links

        self.data = msg.SerializeToString()
        super().__init__(credentials)

    async def handle_response(self, header, payload, writer):
        result = messages_pb2.ReadStreamEventsCompleted()
        self.is_complete = True
        result.ParseFromString(payload)

        if result.result == ReadStreamResult.Success:
            await self.iterator.enqueue_items(
                [_make_event(x) for x in result.events]
            )

            if result.is_end_of_stream:
                self.iterator.finished = True
            else:
                await writer.enqueue(
                    IterStreamEvents(
                        self.stream,
                        batch_size=self.batch_size,
                        from_event=result.next_event_number,
                        resolve_links=self.resolve_links,
                        require_master=self.require_master,
                        direction=self.direction,
                        iterator=self.iterator,
                        correlation_id=uuid4()
                    )
                )
        else:
            assert result.result == ReadStreamResult.NoStream
            msg = "The stream '" + self.stream + "' was not found"
            exn = exceptions.StreamNotFoundException(msg, self.stream)
            await self.iterator.athrow(exn)

    def cancel(self):
        self.iterator.cancel()


class HeartbeatResponse(Operation):
    """Command class for responding to heartbeats.

    Args:
        correlation_id: The unique id of the HeartbeatRequest.
    """
    one_way = True

    def __init__(self, correlation_id, loop=None):
        self.flags = OperationFlags.Empty
        self.command = TcpCommand.HeartbeatResponse
        self.future = Future(loop=loop)
        self.correlation_id = correlation_id or uuid4()
        self.data = bytearray()
        super().__init__()

    def cancel(self):
        self.future.cancel()


class VolatileSubscription:

    def __init__(
            self, stream, initial_commit, initial_event_number, buffer_size
    ):
        self.last_commit_position = initial_commit
        self.last_event_number = initial_event_number
        self.events = StreamingIterator(buffer_size)
        self.stream = stream

    async def enqueue(self, commit_position, event):
        self.last_commit_position = commit_position
        self.last_event_number = event.original_event.event_number
        await self.events.enqueue(event)

    def cancel(self):
        self.events.cancel()


class PersistentSubscription:

    def __init__(
            self,
            conn,
            name,
            stream,
            correlation_id,
            initial_commit,
            initial_event_number,
            buffer_size,
            auto_ack=False
    ):
        self.initial_commit_position = initial_commit
        self.name = name
        self.correlation_id = correlation_id
        self.last_event_number = initial_event_number
        self.events = StreamingIterator(buffer_size)
        self.stream = stream
        self.auto_ack = auto_ack
        self.conn = conn

    async def enqueue(self, event):
        self.last_event_number = event.original_event.event_number
        await self.events.enqueue(event)

        if self.auto_ack:
            await self.conn.ack(event)

    async def ack(self, message: Event):
        await self.conn.ack(
            self.name,
            message.original_event_id,
            correlation_id=self.correlation_id
        )

    def cancel(self):
        self.events.cancel()


class CreateVolatileSubscription(Operation):
    """Command class for creating a non-persistent subscription.

    Args:
        stream: The name of the stream to watch for new events
    """

    def __init__(
            self,
            stream: str,
            resolve_links: bool = True,
            iterator: StreamingIterator = None,
            buffer_size: int = 1,
            credentials: Credential = None,
            correlation_id: UUID = None,
            loop=None
    ) -> None:
        msg = messages_pb2.SubscribeToStream()
        msg.event_stream_id = stream
        msg.resolve_link_tos = resolve_links
        self.stream = stream
        self.command = TcpCommand.SubscribeToStream
        self.flags = OperationFlags.Empty
        self.future: Future = Future(loop=loop)
        self.future.set_result(None)
        self.data = msg.SerializeToString()
        self.correlation_id = correlation_id or uuid4()
        self.is_complete = False
        self.buffer_size = buffer_size
        super().__init__(credentials)

    async def handle_response(self, header, payload, writer):
        if header.cmd == TcpCommand.SubscriptionConfirmation.value:
            result = messages_pb2.SubscriptionConfirmation()
            result.ParseFromString(payload)
            self.subscription = VolatileSubscription(
                self.stream, result.last_commit_position,
                result.last_event_number, self.buffer_size
            )

            self.future.set_result(self.subscription)

        elif header.cmd == TcpCommand.StreamEventAppeared:
            result = messages_pb2.StreamEventAppeared()
            try:
                result.ParseFromString(payload)
                event = _make_event(result.event)
                await self.subscription.enqueue(
                    event.original_event.commit_position, event
                )
            except Exception as e:
                logging.debug(e)
                logging.debug(payload)
                logging.debug(header)

        elif header.cmd == TcpCommand.SubscriptionDropped:
            self.subscription.cancel()

    def cancel(self):
        if not self.future.done():
            self.future.cancel()
        else:
            self.subscription.cancel()


SubscriptionResult = make_enum(
    messages_pb2.
    _CREATEPERSISTENTSUBSCRIPTIONCOMPLETED_CREATEPERSISTENTSUBSCRIPTIONRESULT
)


class SubscriptionCreatedResponse(NamedTuple):
    result: SubscriptionResult
    reason: str


class CreatePersistentSubscription(Operation):

    def __init__(
            self,
            name,
            stream,
            resolve_links=True,
            start_from=-1,
            timeout_ms=8192,
            record_statistics=False,
            live_buffer_size=128,
            read_batch_size=128,
            buffer_size=128,
            max_retry_count=3,
            prefer_round_robin=True,
            checkpoint_after_ms=1024,
            checkpoint_max_count=1024,
            checkpoint_min_count=10,
            subscriber_max_count=10,
            credentials=None,
            correlation_id=None,
            loop=None
    ) -> None:
        self.stream = stream
        self.name = name
        msg = messages_pb2.CreatePersistentSubscription()
        msg.subscription_group_name = name
        msg.event_stream_id = stream
        msg.start_from = start_from
        msg.resolve_link_tos = resolve_links
        msg.message_timeout_milliseconds = timeout_ms
        msg.record_statistics = record_statistics
        msg.live_buffer_size = live_buffer_size
        msg.read_batch_size = read_batch_size
        msg.buffer_size = buffer_size
        msg.max_retry_count = max_retry_count
        msg.prefer_round_robin = prefer_round_robin
        msg.checkpoint_after_time = checkpoint_after_ms
        msg.checkpoint_max_count = checkpoint_max_count
        msg.checkpoint_min_count = checkpoint_min_count
        msg.subscriber_max_count = subscriber_max_count

        self.command = TcpCommand.CreatePersistentSubscription
        self.future: Future = Future(loop=loop)
        self.data = msg.SerializeToString()
        self.correlation_id = correlation_id or uuid4()
        self.is_complete = False
        super().__init__(credentials)

    async def handle_response(self, header, payload, writer):

        result = messages_pb2.CreatePersistentSubscriptionCompleted()
        result.ParseFromString(payload)
        self.future.set_result(
            SubscriptionCreatedResponse(result.result, result.reason)
        )

    def cancel(self):
        self.future.cancel()


class ConnectPersistentSubscription(Operation):

    def __init__(
            self,
            name,
            stream,
            connection,
            max_in_flight=10,
            credentials=None,
            correlation_id=None,
            loop=None
    ) -> None:
        self.stream = stream
        self.max_in_flight = max_in_flight
        self.name = name
        self.conn = connection
        msg = messages_pb2.ConnectToPersistentSubscription()
        msg.subscription_id = name
        msg.event_stream_id = stream
        msg.allowed_in_flight_messages = max_in_flight

        self.command = TcpCommand.ConnectToPersistentSubscription
        self.future: Future = Future(loop=loop)
        self.data = msg.SerializeToString()
        self.correlation_id = correlation_id or uuid4()
        self.is_complete = False
        super().__init__(credentials)

    def createSubscription(self, payload):
        result = messages_pb2.PersistentSubscriptionConfirmation()
        result.ParseFromString(payload)
        self.subscription = PersistentSubscription(
            self.conn, result.subscription_id, self.stream, self.correlation_id,
            result.last_commit_position, result.last_event_number,
            self.max_in_flight
        )

        self.future.set_result(self.subscription)

    async def yieldEvent(self, payload):
        result = messages_pb2.PersistentSubscriptionStreamEventAppeared()
        result.ParseFromString(payload)
        try:
            await self.subscription.enqueue(_make_event(result.event))
        except Exception as e:
            print(e)

    async def handle_response(self, header, payload, writer):
        try:
            if header.cmd == TcpCommand.PersistentSubscriptionConfirmation:
                self.createSubscription(payload)
            elif header.cmd == TcpCommand.PersistentSubscriptionStreamEventAppeared:
                await self.yieldEvent(payload)
        except Exception as e:
            print(e)

    def cancel(self):
        self.future.cancel()


class AcknowledgeMessages(Operation):

    one_way = True

    def __init__(
            self, name, message_ids, correlation_id, credentials=None, loop=None
    ) -> None:
        msg = messages_pb2.PersistentSubscriptionAckEvents()
        msg.subscription_id = name
        msg.processed_event_ids.extend([id.bytes_le for id in message_ids])
        self.command = TcpCommand.PersistentSubscriptionAckEvents
        self.data = msg.SerializeToString()
        self.correlation_id = correlation_id or uuid4()
        super().__init__(credentials)
