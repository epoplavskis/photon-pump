from asyncio import Future, Queue
from collections import namedtuple
from enum import IntEnum
import json
import struct
from typing import Any, Dict, Sequence, Union
from uuid import uuid4, UUID

from . import messages_pb2
from . import exceptions

HEADER_LENGTH = 1 + 1 + 16


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


class StreamDirection(IntEnum):
    Forward = 0
    Backward = 1


class ContentType(IntEnum):
    Json = 0x01
    Binary = 0x00


class OperationFlags(IntEnum):
    Empty = 0x00
    Authenticated = 0x01


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
Header = namedtuple('photonpump_result_header', [
    'size',
    'cmd',
    'flags',
    'correlation_id'])


def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def print_header(header):
    return "%s (%s) of %s flags=%d" % (TcpCommand(header.cmd).name,
                                       header.correlation_id,
                                       sizeof_fmt(header.size),
                                       header.flags)

Header.__repr__ = print_header


NewEventData = namedtuple('photonpump_event', [
    'id',
    'type',
    'data',
    'metadata'])


EventRecord = namedtuple('photonpump_eventrecord', [
    'stream',
    'id',
    'event_number',
    'type',
    'data',
    'metadata',
    'created'])


class StreamingIterator:

    def __init__(self, size):
        self.items = Queue(maxsize=size)
        self.finished = False

    async def __aiter__(self):
        return self

    async def enqueue_items(self, items):
        for item in items:
            await self.items.put(item)

    async def __anext__(self):
        if self.finished and self.items.empty():
            raise StopAsyncIteration()
        return await self.items.get()


class Event(EventRecord):

    def json(self):
        return json.loads(self.data.decode('UTF-8'))


class Operation:
    """The base class for requests to Eventstore.

    Implementors have two responsibilities: they must serialize a byte-stream request
    in the :meth:`~photonpump.messages.Operation.send` method, and they must deserialize and
    handle the response in the :meth:`~photonpump.messages.Operation.handle_response` method.
    """

    def send(self, writer):
        """Write the byte-stream of this request to an instance of StreamWriter"""
        header = self.make_header()
        writer.write(header)
        writer.write(self.data)

    def make_header(self):
        """Build the byte-array representing the operation's header."""
        buf = bytearray()
        data_length = len(self.data)
        buf.extend(struct.pack(
            '<IBB',
            HEADER_LENGTH + data_length,
            self.command,
            self.flags))
        buf.extend(self.correlation_id.bytes)
        return buf

    async def handle_response(self, header, payload, writer):
        """Handle the response from Eventstore.

        Implementors can choose whether to return a single result, return an async
        generator, or send a new Operation to the :class:`photonpump.Connection`.
        """
        pass

    def __repr__(self):
        return "Operation %s (%s)" % (self.__class__.__name__, self.correlation_id)


Pong = namedtuple('photonpump_result_Pong', ['correlation_id'])


class Ping(Operation):
    """Command class for server pings.

    Args:
        correlation_id (optional): A unique identifer for this command.
    """

    def __init__(self, correlation_id: UUID=None, loop=None):
        self.flags = OperationFlags.Empty
        self.command = TcpCommand.Ping
        self.future = Future(loop=loop)
        self.correlation_id = correlation_id or uuid4()
        self.data = bytearray()

    async def handle_response(self, header, payload, writer) -> Pong:
        self.future.set_result(Pong(header.correlation_id))


def NewEvent(type: str,
             id: UUID=uuid4(),
             data: JsonDict=None,
             metadata: JsonDict=None) -> NewEventData:
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
            expected_version: Union[ExpectedVersion, int]=ExpectedVersion.Any,
            require_master: bool=False,
            correlation_id: UUID=None,
            loop=None):
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
            if event.data:
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

    async def handle_response(self, header, payload, writer):
        result = messages_pb2.WriteEventsCompleted()
        result.ParseFromString(payload)
        self.future.set_result(result)


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
            resolve_links: bool=True,
            require_master: bool=False,
            credentials=None,
            correlation_id: UUID=None,
            loop=None):

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

    async def handle_response(self, header, payload, writer):
        result = messages_pb2.ReadEventCompleted()
        result.ParseFromString(payload)
        event = result.event.event

        if result.result == ReadEventResult.Success:
            self.future.set_result(Event(
                event.event_stream_id,
                UUID(bytes_le=event.event_id),
                event.event_number,
                event.event_type,
                event.data,
                event.metadata,
                event.created_epoch))
        elif result.result == ReadEventResult.NoStream:
            msg = "The stream '"+self.stream+"' was not found"
            exn = exceptions.StreamNotFoundException(msg, self.stream)
            self.future.set_exception(exn)


ReadEventResult = make_enum(messages_pb2._READEVENTCOMPLETED_READEVENTRESULT)


ReadStreamResult = make_enum(
        messages_pb2._READSTREAMEVENTSCOMPLETED_READSTREAMRESULT)


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
            max_count: int=100,
            resolve_links: bool=True,
            require_master: bool=False,
            direction: StreamDirection=StreamDirection.Forward,
            credentials=None,
            correlation_id: UUID=None,
            loop=None):

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

    async def handle_response(self, header, payload, writer):
        result = messages_pb2.ReadStreamEventsCompleted()
        try:
            result.ParseFromString(payload)
        except Exception as e:
            print(e)
            print(payload)
            print(header)
            raise
        if result.result == ReadStreamResult.Success:
            self.future.set_result([Event(
                x.event.event_stream_id,
                UUID(bytes_le=x.event.event_id),
                x.event.event_number,
                x.event.event_type,
                x.event.data,
                x.event.metadata,
                x.event.created_epoch) for x in result.events])
        elif result.result == ReadEventResult.NoStream:
            msg = "The stream '"+self.stream+"' was not found"
            exn = exceptions.StreamNotFoundException(msg, self.stream)
            self.future.set_exception(exn)


class IterStreamEvents(Operation):
    """Command class for iterating events from a stream.

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
            batch_size: int=100,
            resolve_links: bool=True,
            require_master: bool=False,
            direction: StreamDirection=StreamDirection.Forward,
            credentials=None,
            correlation_id: UUID=None,
            iterator: StreamingIterator=None,
            loop=None):

        self.correlation_id = correlation_id or uuid4()
        self.batch_size = batch_size
        self.flags = OperationFlags.Empty
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

    async def handle_response(self, header, payload, writer):
        result = messages_pb2.ReadStreamEventsCompleted()
        try:
            result.ParseFromString(payload)
        except Exception as e:
            print(e)
            print(payload)
            print(header)
        if result.result == ReadStreamResult.Success:
            await self.iterator.enqueue_items(
                [Event(
                    x.event.event_stream_id,
                    UUID(bytes_le=x.event.event_id),
                    x.event.event_number,
                    x.event.event_type,
                    x.event.data,
                    x.event.metadata,
                    x.event.created_epoch) for x in result.events])
            await writer.enqueue(IterStreamEvents(
                self.stream,
                batch_size=self.batch_size,
                from_event=result.next_event_number,
                resolve_links=self.resolve_links,
                require_master=self.require_master,
                direction=self.direction,
                iterator=self.iterator,
                correlation_id=uuid4()
                ))

            if result.is_end_of_stream:
                self.iterator.finished = True
        elif result.result == ReadEventResult.NoStream:
            msg = "The stream '"+self.stream+"' was not found"
            exn = exceptions.StreamNotFoundException(msg, self.stream)
            raise exn


class HeartbeatResponse(Operation):
    """Command class for responding to heartbeats.

    Args:
        correlation_id: The unique id of the HeartbeatRequest.
    """

    def __init__(self, correlation_id, loop=None):
        self.flags = OperationFlags.Empty
        self.command = TcpCommand.HeartbeatResponse
        self.future = Future(loop=loop)
        self.correlation_id = correlation_id or uuid4()
        self.data = bytearray()
