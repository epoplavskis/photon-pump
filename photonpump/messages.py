from asyncio import Future
from collections import namedtuple
from enum import IntEnum
import json
import struct
from typing import Any, Dict, Sequence, Union
from uuid import uuid4, UUID

from . import messages_pb2

HEADER_LENGTH = 1 + 1 + 16


class TcpCommand(IntEnum):

    HeartbeatRequest = 0x01
    HeartbeatResponse = 0x02

    Ping = 0x03
    Pong = 0x04

    WriteEvents = 0x82
    WriteEventsCompleted = 0x83


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
        StreamMustNotExist: The request should fail if the stream already exists.
        StreamMustBeEmpty: The request should fail if the stream does not exist, or if the stream already contains events.
        StreamMustExist: The request should fail if the stream does not exist.
    """

    Any = -2
    StreamMustNotExist = -1
    StreamMustBeEmpty = 0
    StreamMustExist = -4


JsonDict = Dict[str, Any]
Header = namedtuple('photonpump_result_header', ['size', 'cmd', 'flags', 'correlation_id'])
NewEventData = namedtuple('photonpump_event', ['id', 'type', 'data', 'metadata'])


class Operation:

    def send(self, writer):
       header = self.make_header(len(self.data), self.command, self.flags, self.correlation_id)
       writer.write(header)
       writer.write(self.data)

    def make_header(self, data_length, cmd, flags, correlation_id):
        buf = bytearray()
        buf.extend(struct.pack('<IBB', HEADER_LENGTH + data_length, cmd, flags))
        buf.extend(correlation_id.bytes)
        return buf


class Ping(Operation):
    """Command class for server pings.

    Args:
        correlation_id (optional): A unique identifer for this command.
    """

    def __init__(self, correlation_id:UUID=uuid4(), loop=None):
        self.flags = OperationFlags.Empty
        self.command = TcpCommand.Ping
        self.future = Future(loop=loop)
        self.correlation_id = correlation_id
        self.data = bytearray()


Pong = namedtuple('photonpump_result_Pong', ['correlation_id'])


def NewEvent(type:str,
             id:UUID=uuid4(),
             data:JsonDict=None,
             metadata:JsonDict=None) -> NewEventData:
    """Build the data structure for a new event.

    Args:
        type: An event type.
        id: The uuid identifier for the event.
        data: A dict containing data for the event. These data must be json serializable.
        metadata: A dict containing metadata about the event. These must be json serializable.
    """
    return NewEventData(id, type, data, metadata)


class WriteEvents(Operation):
    """Command class for writing a sequence of events to a single stream.

    Args:
        stream: The name of the stream to write to.
        events: A sequence of events to write.
        expected_version (optional): The expected version of the target stream used for concurrency control.
        required_master (optional): True if this command must be sent direct to the master node, otherwise False.
        correlation_id (optional): A unique identifer for this command.

    """

    def __init__(self,
            stream:str,
            events:Sequence[NewEventData],
            expected_version:Union[ExpectedVersion,int]=ExpectedVersion.Any,
            require_master:bool=False,
            correlation_id:UUID=uuid4(),
            loop=None):
       self.correlation_id = correlation_id
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
           e.data_content_type = ContentType.Json if event.data else ContentType.Binary
           e.data = json.dumps(event.data).encode('UTF-8') if event.data else bytes()
           e.metadata_content_type = ContentType.Json if event.metadata else ContentType.Binary
           e.metadata = json.dumps(event.metadata).encode('UTF-8') if event.metadata else bytes()

       self.data = msg.SerializeToString()


class HeartbeatResponse(Operation):
    """Command class for responding to heartbeats.

    Args:
        correlation_id: The unique id of the HeartbeatRequest.
    """

    def __init__(self, correlation_id, loop=None):
        self.flags = OperationFlags.Empty
        self.command = TcpCommand.HeartbeatResponse
        self.future = Future(loop=loop)
        self.correlation_id = correlation_id
        self.data = bytearray()


