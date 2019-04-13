import datetime
import json
import math
import struct
from collections import namedtuple
from enum import IntEnum
from typing import Any, Dict, Sequence, NamedTuple, Optional
from uuid import UUID, uuid4

from . import messages_pb2

HEADER_LENGTH = 1 + 1 + 16
SIZE_UINT_32 = 4
_LENGTH = struct.Struct("<I")
_HEAD = struct.Struct("<BBIIII")

ROUND_ROBIN = "RoundRobin"
DISPATCH_TO_SINGLE = "DisptchToSingle"
PINNED = "Pinned"


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
    UnsubscribeFromStream = 0xC3
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

    BadRequest = 0xF0
    NotHandled = 0xF1
    Authenticate = 0xF2
    Authenticated = 0xF3
    NotAuthenticated = 0xF4
    IdentifyClient = 0xF5
    ClientIdentified = 0xF6


class StreamDirection(IntEnum):
    Forward = 0
    Backward = 1


class ContentType(IntEnum):
    Json = 0x01
    Binary = 0x00


class OperationFlags(IntEnum):
    Empty = 0x00
    Authenticated = 0x01


class Position(NamedTuple):

    commit: int
    prepare: int

    @classmethod
    def for_direction(cls, direction, pos):
        if pos is None:
            return (
                Position(0, 0)
                if direction == StreamDirection.Forward
                else Position(-1, -1)
            )
        elif pos is Beginning:
            return Position(0, 0)
        elif pos is End:
            return Position(-1, -1)

        return pos


Position.min = Position(-1, -1)


class _PositionSentinel:
    """
    Provides magic values for beginning/end
    """

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "Position: " + self.name


Beginning = _PositionSentinel("Beginning")
End = _PositionSentinel("End")

OperationResult = make_enum(messages_pb2._OPERATIONRESULT)
NotHandledReason = make_enum(messages_pb2._NOTHANDLED_NOTHANDLEDREASON)
SubscriptionDropReason = make_enum(
    messages_pb2._SUBSCRIPTIONDROPPED_SUBSCRIPTIONDROPREASON
)


class Credential:
    def __init__(self, username, password):
        self.username = username
        self.password = password

        username_bytes = username.encode("UTF-8")
        password_bytes = password.encode("UTF-8")

        self.length = 2 + len(username_bytes) + len(password_bytes)
        self.bytes = bytearray()

        self.bytes.extend(len(username_bytes).to_bytes(1, byteorder="big"))
        self.bytes.extend(username_bytes)
        self.bytes.extend(len(password_bytes).to_bytes(1, byteorder="big"))
        self.bytes.extend(password_bytes)

    @classmethod
    def from_bytes(cls, data):
        """
        I am so sorry.
        """
        len_username = int.from_bytes(data[0:2], byteorder="big")
        offset_username = 2 + len_username
        username = data[2:offset_username].decode("UTF-8")
        offset_password = 2 + offset_username
        len_password = int.from_bytes(
            data[offset_username:offset_password], byteorder="big"
        )
        pass_begin = offset_password
        pass_end = offset_password + len_password
        password = data[pass_begin:pass_end].decode("UTF-8")

        return cls(username, password)


class InboundMessage:
    def __init__(
        self, conversation_id: UUID, command: TcpCommand, payload: bytes = None
    ) -> None:
        self.conversation_id = conversation_id
        self.command = command
        self.payload = payload or b""
        self.data_length = len(payload)
        self.length = HEADER_LENGTH + self.data_length

    @property
    def header_bytes(self):
        data = bytearray(SIZE_UINT_32 + 2)
        struct.pack_into("<IBB", data, 0, self.length, self.command, 0)
        data.extend(self.conversation_id.bytes_le)

        return data

    def __repr__(self):
        return self.__str__() + "\n" + dump(self.header_bytes, self.payload)

    def __str__(self):
        return "%s (%s) of %s flags=%d" % (
            TcpCommand(self.command).name,
            self.conversation_id,
            sizeof_fmt(self.length),
            0,
        )


class OutboundMessage:
    def __init__(
        self,
        conversation_id: UUID,
        command: TcpCommand,
        payload: Any,
        creds: Credential = None,
        one_way: bool = False,
        require_master=False,
    ) -> None:
        self.conversation_id = conversation_id
        self.command = command
        self.payload = payload
        self.creds = creds
        self.require_master = require_master

        self.data_length = len(payload)
        self.length = HEADER_LENGTH + self.data_length

        if self.creds:
            self.length += creds.length
        self.one_way = one_way

    @property
    def header_bytes(self):
        data = bytearray(SIZE_UINT_32 + 2)
        struct.pack_into(
            "<IBB",
            data,
            1 if self.require_master else 0,
            self.length,
            self.command.value,
            1 if self.creds else 0,
        )
        data.extend(self.conversation_id.bytes_le)

        if self.creds:
            data.extend(self.creds.bytes)

        return data

    def __repr__(self):
        return dump(self.header_bytes, self.payload)

    def __str__(self):
        return "%s (%s) of %s flags=%d" % (
            TcpCommand(self.command).name,
            self.conversation_id,
            sizeof_fmt(self.length),
            0,
        )

    def __eq__(self, other):
        return isinstance(other, OutboundMessage) and repr(self) == repr(other)


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
    "photonpump_result_header",
    ["size", "cmd", "flags", "correlation_id", "username", "password"],
)


def parse_header(length: bytearray, data: bytearray) -> Header:
    (size,) = _LENGTH.unpack(length)

    return Header(size, data[0], data[1], UUID(bytes_le=data[2:18]), None, None)


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0

    return "%.1f%s%s" % (num, "Yi", suffix)


def print_header(header):
    return "%s (%s) of %s flags=%d" % (
        TcpCommand(header.cmd).name,
        header.correlation_id,
        sizeof_fmt(header.size),
        header.flags,
    )


Header.__repr__ = print_header
Header.__new__.__defaults__ = (None, None)

NewEventData = namedtuple("photonpump_event", ["id", "type", "data", "metadata"])

EventRecord = namedtuple(
    "photonpump_eventrecord",
    ["stream", "id", "event_number", "type", "data", "metadata", "created"],
)


def _json(self):
    return json.loads(self.data.decode("UTF-8"))


EventRecord.json = _json


class Event:
    def __init__(
        self, event: EventRecord, link: EventRecord, position: Optional[Position] = None
    ) -> None:
        self.event = event
        self.link = link
        self.stream = event.stream
        self.id = event.id
        self.event_number = self.received_event.event_number
        self.type = event.type
        self.data = event.data
        self.metadata = event.metadata
        self.created = event.created
        self.position = position

    @property
    def received_event(self) -> EventRecord:
        return self.link or self.event

    def json(self):
        return json.loads(self.data.decode("UTF-8"))

    def __repr__(self):
        return f"<Event {self.created}: {self.event_number}@{self.stream}:{self.type}>"


class StreamSlice(list):
    def __init__(
        self,
        events: Sequence[Event],
        next_event_number: int,
        last_event_number: int,
        prepare_position: int = None,
        commit_position: int = None,
        is_end_of_stream: bool = False,
    ) -> None:
        super().__init__(events)
        self.next_event_number = next_event_number
        self.last_event_number = last_event_number
        self.prepare_position = prepare_position
        self.commit_position = commit_position
        self.is_end_of_stream = is_end_of_stream
        self.events = events


class AllStreamSlice(list):
    def __init__(
        self, events: Sequence[Event], position: Position, next_position: Position
    ) -> None:
        super().__init__(events)
        self.next_position = next_position
        self.start_position = position
        self.events = events


def dump(*chunks: bytearray):
    data = bytearray()

    for chunk in chunks:
        data.extend(chunk)

    length = len(data)
    rows = length / 16

    dump = []

    for i in range(0, math.ceil(rows)):
        offset = i * 16
        row = data[offset : offset + 16]
        hex = "{0: <47}".format(" ".join("{:02x}".format(x) for x in row))
        dump.append("%06d: %s | %a" % (offset, hex, bytes(row)))

    return "\n" + "\n".join(dump)


def _make_event(record: messages_pb2.ResolvedEvent):

    if type(record) == messages_pb2.ResolvedEvent:
        position = Position(record.commit_position, record.prepare_position)
    else:
        position = None

    link = (
        EventRecord(
            record.link.event_stream_id,
            UUID(bytes_le=bytes(record.link.event_id)),
            record.link.event_number,
            record.link.event_type,
            record.link.data,
            record.link.metadata,
            datetime.datetime.fromtimestamp(record.link.created_epoch / 1e3),
        )
        if record.HasField("link")
        else None
    )

    if not record.HasField("event"):
        return Event(link, None)

    event = EventRecord(
        record.event.event_stream_id,
        UUID(bytes_le=bytes(record.event.event_id)),
        record.event.event_number,
        record.event.event_type,
        record.event.data,
        record.event.metadata,
        datetime.datetime.fromtimestamp(record.event.created_epoch / 1e3),
    )

    return Event(event, link, position)


def NewEvent(
    type: str, id: UUID = None, data: JsonDict = None, metadata: JsonDict = None
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

    return NewEventData(id or uuid4(), type, data, metadata)


ReadEventResult = make_enum(messages_pb2._READEVENTCOMPLETED_READEVENTRESULT)

ReadStreamResult = make_enum(messages_pb2._READSTREAMEVENTSCOMPLETED_READSTREAMRESULT)

ReadAllResult = make_enum(messages_pb2._READALLEVENTSCOMPLETED_READALLRESULT)

SubscriptionResult = make_enum(
    messages_pb2._CREATEPERSISTENTSUBSCRIPTIONCOMPLETED_CREATEPERSISTENTSUBSCRIPTIONRESULT
)


class SubscriptionCreatedResponse:
    def __init__(self, result: SubscriptionResult, reason: str) -> None:
        self.reason = reason
        self.result = result
