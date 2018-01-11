import json
import logging
from asyncio import Future
from enum import IntEnum
from typing import Any, NamedTuple, Sequence, Union
from uuid import UUID, uuid4

from photonpump import messages_pb2 as proto
from photonpump import exceptions
from photonpump.messages import (
    ContentType, Credential, Event, ExpectedVersion, InboundMessage, NewEvent,
    OutboundMessage, ReadEventResult, ReadStreamResult, StreamDirection,
    StreamSlice, TcpCommand, _make_event, NotHandledReason
)


class ReplyAction(IntEnum):

    CompleteScalar = 0
    CompleteError = 1

    BeginIterator = 2
    YieldToIterator = 3
    CompleteIterator = 4
    RaiseToIterator = 5

    BeginVolatileSubscription = 6
    YieldToSubscription = 7
    FinishSubscription = 8
    RaiseToSubscription = 8


class Reply(NamedTuple):

    action: ReplyAction
    result: Any
    next_message: OutboundMessage


class Conversation:

    def __init__(
            self, conversation_id: UUID = None, credential: Credential = None
    ) -> None:
        self.conversation_id = conversation_id or uuid4()
        self.result: Future = Future()
        self.is_complete = False
        self.credential = credential

    def reply(self, response: InboundMessage) -> Reply:
        pass

    def error(self, exn: Exception):
        return Reply(ReplyAction.CompleteError, exn, None)

    def conversation_error(self, exn_type, response) -> Reply:
        error = response.payload.decode('UTF-8')
        exn = exn_type(self.conversation_id, error)

        return self.error(exn)

    def unhandled_message(self, response):
        body = proto.NotHandled()
        body.ParseFromString(response.payload)

        if body.reason == NotHandledReason.NotReady:
            exn = exceptions.NotReady(self.conversation_id)
        elif body.reason == NotHandledReason.TooBusy:
            exn = exceptions.TooBusy(self.conversation_id)
        elif body.reason == NotHandledReason.NotMaster:
            exn = exceptions.NotMaster(self.conversation_id)
        else:
            exn = exceptions.NotHandled(self.conversation_id, body.reason)

        return self.error(exn)

    def respond_to(self, response: InboundMessage) -> Reply:
        try:
            if response.command is TcpCommand.BadRequest:
                return self.conversation_error(exceptions.BadRequest, response)
            elif response.command is TcpCommand.NotAuthenticated:
                return self.conversation_error(
                    exceptions.NotAuthenticated, response
                )
            elif response.command is TcpCommand.NotHandled:
                return self.unhandled_message(response)
            else:
                return self.reply(response)
        except Exception as exn:
            return self.error(
                exceptions.PayloadUnreadable(
                    self.conversation_id, response.payload, exn
                )
            )

        return None


class Heartbeat(Conversation):

    def __init__(self, conversation_id: UUID) -> None:
        super().__init__(conversation_id)

    def start(self):
        return OutboundMessage(
            self.conversation_id, TcpCommand.HeartbeatResponse, b''
        )


class Ping(Conversation):

    def start(self):
        return OutboundMessage(self.conversation_id, TcpCommand.Ping, b'')

    def reply(self, _: InboundMessage):
        return Reply(ReplyAction.CompleteScalar, None, None)


class WriteEventsConversation(Conversation):
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
            events: Sequence[NewEvent],
            expected_version: Union[ExpectedVersion, int] = ExpectedVersion.Any,
            require_master: bool = False,
            conversation_id: UUID = None,
            credential=None,
            loop=None
    ):
        super().__init__(conversation_id, credential)
        self.stream = stream
        self.require_master = require_master
        self.events = events
        self.expected_version = expected_version

    def start(self):
        msg = proto.WriteEvents()
        msg.event_stream_id = self.stream
        msg.require_master = self.require_master
        msg.expected_version = self.expected_version

        for event in self.events:
            e = msg.events.add()
            e.event_id = event.id.bytes_le
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

        data = msg.SerializeToString()

        return OutboundMessage(
            self.conversation_id, TcpCommand.WriteEvents, data, self.credential
        )

    def reply(self, response: InboundMessage):
        result = proto.WriteEventsCompleted()
        result.ParseFromString(response.payload)
        self.is_complete = True
        self.result.set_result(result)


class ReadStreamEventsBehaviour:

    def __init__(self, result_type, response_cls):
        self.result_type = result_type
        self.response_cls = response_cls

    def success(self, result):
        pass

    def error(self, exn: Exception):
        pass

    def reply(self, response: InboundMessage):
        result = self.response_cls()
        result.ParseFromString(response.payload)

        if result.result == self.result_type.Success:
            return self.success(result)
        elif result.result == self.result_type.NoStream:
            return self.error(
                exceptions.StreamNotFound(self.conversation_id, self.stream)
            )
        elif result.result == self.result_type.StreamDeleted:
            return self.error(
                exceptions.StreamDeleted(self.conversation_id, self.stream)
            )
        elif result.result == self.result_type.Error:
            return self.error(
                exceptions.ReadError(
                    self.conversation_id, self.stream, result.error
                )
            )
        elif result.result == self.result_type.AccessDenied:
            return self.error(
                exceptions.AccessDenied(
                    self.conversation_id,
                    type(self).__name__,
                    result.error,
                    stream=self.stream
                )
            )
        elif self.result_type == ReadEventResult and result.result == self.result_type.NotFound:
            return self.error(
                exceptions.EventNotFound(
                    self.conversation_id, self.stream, self.event_number
                )
            )


class ReadEvent(ReadStreamEventsBehaviour, Conversation):
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
            conversation_id: UUID = None,
            credentials=None
    ) -> None:

        Conversation.__init__(self, conversation_id, credential=credentials)
        ReadStreamEventsBehaviour.__init__(
            self, ReadEventResult, proto.ReadEventCompleted
        )
        self.stream = stream
        self.event_number = event_number
        self.require_master = require_master
        self.resolve_link_tos = resolve_links

    def start(self) -> OutboundMessage:
        msg = proto.ReadEvent()
        msg.event_number = self.event_number
        msg.event_stream_id = self.stream
        msg.require_master = self.require_master
        msg.resolve_link_tos = self.resolve_link_tos

        data = msg.SerializeToString()

        return OutboundMessage(
            self.conversation_id, TcpCommand.Read, data, self.credential
        )

    def success(self, response):
        return Reply(
            ReplyAction.CompleteScalar, _make_event(response.event), None
        )

    def error(self, exn):
        return Reply(ReplyAction.CompleteError, exn, None)


class ReadStreamEventsConversation(ReadStreamEventsBehaviour, Conversation):
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
            from_event: int = 0,
            max_count: int = 100,
            resolve_links: bool = True,
            require_master: bool = False,
            direction: StreamDirection = StreamDirection.Forward,
            credentials=None,
            conversation_id: UUID = None
    ) -> None:

        Conversation.__init__(self, conversation_id, credential=credentials)
        ReadStreamEventsBehaviour.__init__(
            self, ReadStreamResult, proto.ReadStreamEventsCompleted
        )
        self.stream = stream
        self.direction = direction
        self.from_event = from_event
        self.max_count = max_count
        self.require_master = require_master
        self.resolve_link_tos = resolve_links

    def _fetch_page_message(self, from_event):
        if self.direction == StreamDirection.Forward:
            command = TcpCommand.ReadStreamEventsForward
        else:
            command = TcpCommand.ReadStreamEventsBackward

        msg = proto.ReadStreamEvents()
        msg.event_stream_id = self.stream
        msg.from_event_number = from_event
        msg.max_count = self.max_count
        msg.require_master = self.require_master
        msg.resolve_link_tos = self.resolve_link_tos

        data = msg.SerializeToString()

        return OutboundMessage(
            self.conversation_id, command, data, self.credential
        )

    def start(self):
        return self._fetch_page_message(self.from_event)

    def success(self, result: proto.ReadStreamEventsCompleted):
        events = [_make_event(x) for x in result.events]

        return Reply(
            ReplyAction.CompleteScalar,
            StreamSlice(
                events, result.next_event_number, result.last_event_number,
                None, result.last_commit_position, result.is_end_of_stream
            ), None
        )

    def error(self, exn):
        return Reply(ReplyAction.CompleteError, exn, None)


class IterStreamEvents(ReadStreamEventsBehaviour, Conversation):
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
            from_event: int = 0,
            batch_size: int = 100,
            resolve_links: bool = True,
            require_master: bool = False,
            direction: StreamDirection = StreamDirection.Forward,
            credentials=None,
            conversation_id: UUID = None
    ):

        Conversation.__init__(self, conversation_id, credentials)
        ReadStreamEventsBehaviour.__init__(
            self, ReadStreamResult, proto.ReadStreamEventsCompleted
        )
        self.batch_size = batch_size
        self.waiting_for_page = 0
        self.stream = stream
        self.resolve_links = resolve_links
        self.require_master = require_master
        self.direction = direction
        self.from_event = from_event

        if direction == StreamDirection.Forward:
            self.command = TcpCommand.ReadStreamEventsForward
        else:
            self.command = TcpCommand.ReadStreamEventsBackward

    def _fetch_page_message(self, from_event):
        if self.direction == StreamDirection.Forward:
            command = TcpCommand.ReadStreamEventsForward
        else:
            command = TcpCommand.ReadStreamEventsBackward

        msg = proto.ReadStreamEvents()
        msg.event_stream_id = self.stream
        msg.from_event_number = from_event
        msg.max_count = self.batch_size
        msg.require_master = self.require_master
        msg.resolve_link_tos = self.resolve_links

        data = msg.SerializeToString()

        return OutboundMessage(
            self.conversation_id, command, data, self.credential
        )

    def start(self):
        return self._fetch_page_message(self.from_event)

    def success(self, result: proto.ReadStreamEventsCompleted):
        events = [_make_event(x) for x in result.events]

        next_message = self._fetch_page_message(
            result.next_event_number
        ) if not result.is_end_of_stream else None

        self.waiting_for_page += 1

        return Reply(
            ReplyAction.BeginIterator,
            StreamSlice(
                events, result.next_event_number, result.last_event_number,
                None, result.last_commit_position, result.is_end_of_stream
            ), next_message
        )

    def error(self, exn: Exception) -> Reply:
        if self.waiting_for_page > 0:
            return Reply(ReplyAction.RaiseToIterator, exn, None)

        return Reply(ReplyAction.CompleteError, exn, None)


class VolatileSubscription:

    def __init__(
            self, stream, initial_commit, initial_event_number, buffer_size
    ):
        self.last_commit_position = initial_commit
        self.last_event_number = initial_event_number
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


class CreateVolatileSubscription(Conversation):
    """Command class for creating a non-persistent subscription.

    Args:
        stream: The name of the stream to watch for new events
    """

    class State(IntEnum):
        init = 0
        catch_up = 1
        live = 2

    def __init__(
            self,
            stream: str,
            resolve_links: bool = True,
            buffer_size: int = 1,
            credentials: Credential = None,
            conversation_id: UUID = None
    ) -> None:
        super().__init__(conversation_id, credentials)
        self.stream = stream
        self.buffer_size = buffer_size
        self.resolve_links = resolve_links
        self.state = CreateVolatileSubscription.State.init

    def error(self, exn):
        return Reply(ReplyAction.RaiseToSubscription, exn, None)

    def start(self):
        msg = proto.SubscribeToStream()
        msg.event_stream_id = self.stream
        msg.resolve_link_tos = self.resolve_links

        return OutboundMessage(
            self.conversation_id, TcpCommand.SubscribeToStream,
            msg.SerializeToString()
        )

    def reply_from_init(self, response: InboundMessage):
        result = proto.SubscriptionConfirmation()
        result.ParseFromString(response.payload)

        self.state = CreateVolatileSubscription.State.live

        return Reply(
            ReplyAction.BeginVolatileSubscription,
            VolatileSubscription(
                self.stream, result.last_commit_position,
                result.last_event_number, self.buffer_size
            ), None
        )

    def reply_from_live(self, response: InboundMessage):
        result = proto.StreamEventAppeared()
        result.ParseFromString(response.payload)

        return Reply(
            ReplyAction.YieldToSubscription, _make_event(result.event), None
        )

    def reply(self, response: InboundMessage):

        if self.state == CreateVolatileSubscription.State.init:
            return self.reply_from_init(response)

        if self.state == CreateVolatileSubscription.State.live:
            return self.reply_from_live(response)