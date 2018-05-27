import json
import logging
import time
from asyncio import Future, Queue, TimeoutError
from enum import IntEnum
from typing import Any, NamedTuple, Optional, Sequence, Union
from uuid import UUID, uuid4

from google.protobuf.text_format import MessageToString

from photonpump import exceptions
from photonpump import messages as messages
from photonpump import messages_pb2 as proto
from photonpump.messages import (
    ContentType, Credential, ExpectedVersion, InboundMessage, NewEvent,
    NotHandledReason, OutboundMessage, ReadEventResult, ReadStreamResult,
    StreamDirection, StreamSlice, SubscriptionResult, TcpCommand, _make_event
)


class ReplyAction(IntEnum):

    CompleteError = 1
    CancelFuture = 2

    BeginVolatileSubscription = 7
    YieldToSubscription = 8
    FinishSubscription = 9
    RaiseToSubscription = 10

    BeginPersistentSubscription = 11
    ContinueSubscription = 12

    ResubmitMessage = 22


class Reply(NamedTuple):

    action: ReplyAction
    result: Any
    next_message: OutboundMessage


class StreamingIterator:

    def __init__(self, size=0):
        self.items = Queue(size)
        self.finished = False
        self.fut = None

    async def __aiter__(self):
        return self

    async def enqueue_items(self, items):

        for item in items:
            await self.items.put(item)

    async def enqueue(self, item):
        await self.items.put(item)

    async def anext(self):
        return await self.__anext__()

    async def __anext__(self):

        if self.finished and self.items.empty():
            raise StopAsyncIteration()
        try:
            _next = await self.items.get()
        except Exception as e:
            raise StopAsyncIteration()

        if isinstance(_next, StopIteration):
            raise StopAsyncIteration()

        if isinstance(_next, Exception):
            raise _next

        return _next

    async def asend(self, item):
        await self.items.put(item)


class MagicConversation:

    def __init__(
            self,
            conversation_id: Optional[UUID] = None,
            credential: Optional[Credential] = None
    ) -> None:
        self.conversation_id = conversation_id or uuid4()
        self.result: Future = Future()
        self.is_complete = False
        self.credential = credential
        self._logger = logging.get_named_logger(Conversation)
        self.one_way = False

    def __str__(self):
        return "<%s %s>" % (type(self).__name__, self.conversation_id)

    def __eq__(self, other):
        if not isinstance(other, MagicConversation):
            return False

        return self.conversation_id == other.conversation_id

    async def start(self, output: Queue) -> Future:
        pass

    async def reply(self, message: InboundMessage, output: Queue) -> None:
        pass

    async def error(self, exn: Exception) -> None:
        self.is_complete = True
        self.result.set_exception(exn)

    def expect_only(self, command: TcpCommand, response: InboundMessage):
        if response.command != command:
            raise exceptions.UnexpectedCommand(command, response.command)

    async def respond_to(self, response: InboundMessage, output: Queue) -> None:
        try:
            if response.command is TcpCommand.BadRequest:
                return await self.conversation_error(
                    exceptions.BadRequest, response
                )

            if response.command is TcpCommand.NotAuthenticated:
                return await self.conversation_error(
                    exceptions.NotAuthenticated, response
                )

            if response.command is TcpCommand.NotHandled:
                return await self.unhandled_message(response)

            return await self.reply(response, output)
        except Exception as exn:
            self._logger.error('Failed to read server response', exc_info=True)

            return await self.error(
                exceptions.PayloadUnreadable(
                    self.conversation_id, response.payload, exn
                )
            )

    async def unhandled_message(self, response) -> None:
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

        return await self.error(exn)

    async def conversation_error(self, exn_type, response) -> None:
        error = response.payload.decode('UTF-8')
        exn = exn_type(self.conversation_id, error)

        return await self.error(exn)


class Conversation:

    def __init__(
            self,
            conversation_id: Optional[UUID] = None,
            credential: Optional[Credential] = None
    ) -> None:
        self.conversation_id = conversation_id or uuid4()
        self.result: Future = Future()
        self.is_complete = False
        self.credential = credential
        self._logger = logging.get_named_logger(Conversation)

    def __str__(self):
        return "<Conversation %s (%s)>" % (type(self), self.conversation_id)

    def __eq__(self, other):
        if not isinstance(other, Conversation):
            return False

        return self.conversation_id == other.conversation_id

    def start(self) -> OutboundMessage:
        pass

    def reply(self, response: InboundMessage) -> Reply:
        pass

    def error(self, exn: Exception):
        return Reply(ReplyAction.CompleteError, exn, None)

    def expect_only(self, command: TcpCommand, response: InboundMessage):
        if response.command != command:
            raise exceptions.UnexpectedCommand(command, response.command)

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

    def timeout(self):
        return self.error(TimeoutError())

    def stop(self):
        return Reply(ReplyAction.CancelFuture, None, None)

    def respond_to(self, response: InboundMessage) -> Reply:
        try:
            if response.command is TcpCommand.BadRequest:
                return self.conversation_error(exceptions.BadRequest, response)

            if response.command is TcpCommand.NotAuthenticated:
                return self.conversation_error(
                    exceptions.NotAuthenticated, response
                )

            if response.command is TcpCommand.NotHandled:
                return self.unhandled_message(response)

            return self.reply(response)
        except Exception as exn:
            self._logger.error('Failed to read server response', exc_info=True)

            return self.error(
                exceptions.PayloadUnreadable(
                    self.conversation_id, response.payload, exn
                )
            )


class TimerConversation(MagicConversation):

    def __init__(self, conversation_id, credential):
        super().__init__(conversation_id, credential)
        self.started_at = time.perf_counter()

    async def start(self, output: Queue) -> None:
        self.started_at = time.perf_counter()

    async def reply(self, message: InboundMessage, output: Queue) -> None:
        logging.info("Replying from conversation %s", self)
        responded_at = time.perf_counter()
        self.result.set_result(self.started_at - responded_at)
        self.is_complete = True


class Heartbeat(TimerConversation):

    INBOUND = 0
    OUTBOUND = 1

    def __init__(
            self, conversation_id: UUID, direction=INBOUND, credential=None
    ) -> None:
        super().__init__(conversation_id, credential=None)
        self.direction = direction
        self.result = Future()

    async def start(self, output: Queue) -> Future:

        await super().start(output)

        if self.direction == Heartbeat.INBOUND:
            one_way = True
            cmd = TcpCommand.HeartbeatResponse
        else:
            one_way = False
            cmd = TcpCommand.HeartbeatRequest

        await output.put(
            OutboundMessage(
                self.conversation_id,
                cmd,
                b'',
                self.credential,
                one_way=one_way
            )
        )

    async def reply(self, message: InboundMessage, output: Queue) -> None:
        self.expect_only(TcpCommand.HeartbeatResponse, message)
        await super().reply(message, output)


class Ping(TimerConversation):

    def __init__(self, conversation_id: UUID = None, credential=None) -> None:
        super().__init__(conversation_id or uuid4(), credential)

    async def start(self, output: Queue) -> Future:
        await super().start(output)

        if output:
            await output.put(
                OutboundMessage(
                    self.conversation_id, TcpCommand.Ping, b'', self.credential
                )
            )

        return self.result

    async def reply(self, message: InboundMessage, output: Queue) -> None:
        self.expect_only(TcpCommand.Pong, message)
        await super().reply(message, output)


class WriteEvents(MagicConversation):
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
        self._logger = logging.get_named_logger(WriteEvents)
        self.stream = stream
        self.require_master = require_master
        self.events = events
        self.expected_version = expected_version

    async def start(self, output: Queue) -> None:
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

        await output.put(OutboundMessage(
            self.conversation_id, TcpCommand.WriteEvents, data, self.credential
        ))

    async def reply(self, message: InboundMessage, output: Queue) -> None:
        self.expect_only(TcpCommand.WriteEventsCompleted, message)
        result = proto.WriteEventsCompleted()
        result.ParseFromString(message.payload)

        self.result.set_result(result)

    def timeout(self):
        return Reply(ReplyAction.ResubmitMessage, None, self.start())


class ReadStreamEventsBehaviour:

    def __init__(self, result_type, response_cls):
        self.result_type = result_type
        self.response_cls = response_cls

    def success(self, result, output: Queue):
        pass

    async def reply(self, message: InboundMessage, output: Queue):
        logging.error("HERRO")
        result = self.response_cls()
        result.ParseFromString(message.payload)

        if result.result == self.result_type.Success:
            logging.error(message)
            await self.success(result, output)
        elif result.result == self.result_type.NoStream:
            await self.error(
                exceptions.StreamNotFound(self.conversation_id, self.stream)
            )
        elif result.result == self.result_type.StreamDeleted:
            await self.error(
                exceptions.StreamDeleted(self.conversation_id, self.stream)
            )
        elif result.result == self.result_type.Error:
            await self.error(
                exceptions.ReadError(
                    self.conversation_id, self.stream, result.error
                )
            )
        elif result.result == self.result_type.AccessDenied:
            await self.error(
                exceptions.AccessDenied(
                    self.conversation_id,
                    type(self).__name__,
                    result.error,
                    stream=self.stream
                )
            )
        elif self.result_type == ReadEventResult and result.result == self.result_type.NotFound:
            await self.error(
                exceptions.EventNotFound(
                    self.conversation_id, self.stream, self.event_number
                )
            )


class ReadEvent(ReadStreamEventsBehaviour, MagicConversation):
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
            conversation_id: Optional[UUID] = None,
            credentials=None
    ) -> None:

        MagicConversation.__init__(self, conversation_id, credential=credentials)
        ReadStreamEventsBehaviour.__init__(
            self, ReadEventResult, proto.ReadEventCompleted
        )
        self.stream = stream
        self.event_number = event_number
        self.require_master = require_master
        self.resolve_link_tos = resolve_links

    async def start(self, output: Queue) -> None:
        msg = proto.ReadEvent()
        msg.event_number = self.event_number
        msg.event_stream_id = self.stream
        msg.require_master = self.require_master
        msg.resolve_link_tos = self.resolve_link_tos

        data = msg.SerializeToString()

        await output.put(OutboundMessage(
            self.conversation_id, TcpCommand.Read, data, self.credential
        ))

    async def success(self, response, output: Queue):
        self.is_complete = True
        self.result.set_result(_make_event(response.event))


class ReadStreamEvents(ReadStreamEventsBehaviour, MagicConversation):
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

        MagicConversation.__init__(self, conversation_id, credential=credentials)
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

    async def start(self, output):
        await output.put(self._fetch_page_message(self.from_event))

    async def success(self, result: proto.ReadStreamEventsCompleted, output: Queue):
        events = [_make_event(x) for x in result.events]

        self.is_complete = True
        self.result.set_result(
            StreamSlice(
                events, result.next_event_number, result.last_event_number,
                None, result.last_commit_position, result.is_end_of_stream
            )
        )


class IterStreamEvents(ReadStreamEventsBehaviour, MagicConversation):
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
            from_event: int = None,
            batch_size: int = 100,
            resolve_links: bool = True,
            require_master: bool = False,
            direction: StreamDirection = StreamDirection.Forward,
            credentials=None,
            conversation_id: UUID = None
    ):

        MagicConversation.__init__(self, conversation_id, credentials)
        ReadStreamEventsBehaviour.__init__(
            self, ReadStreamResult, proto.ReadStreamEventsCompleted
        )
        self.batch_size = batch_size
        self.has_first_page = False
        self.stream = stream
        self.resolve_links = resolve_links
        self.require_master = require_master
        self.direction = direction
        self._logger = logging.get_named_logger(IterStreamEvents)
        self.iterator = StreamingIterator(self.batch_size)

        if direction == StreamDirection.Forward:
            self.command = TcpCommand.ReadStreamEventsForward
            self.from_event = from_event or 0
        else:
            self.command = TcpCommand.ReadStreamEventsBackward
            self.from_event = from_event or -1

    def _fetch_page_message(self, from_event):
        self._logger.debug(
            "Requesting page of %d events from number %d", self.batch_size,
            self.from_event
        )

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

    async def start(self, output: Queue):
        await output.put(self._fetch_page_message(self.from_event))

    async def success(self, result: proto.ReadStreamEventsCompleted, output: Queue):
        events = [_make_event(x) for x in result.events]
        await self.iterator.enqueue_items(events)
        if not self.has_first_page:
            self.result.set_result(self.iterator)
            self.has_first_page = True

        if result.is_end_of_stream:
            self.is_complete = True
            await self.iterator.asend(StopAsyncIteration())
        else:
            await output.put(self._fetch_page_message(result.next_event_number))

    async def error(self, exn: Exception) -> None:
        self.is_complete = True
        if self.has_first_page:
            await self.iterator.asend(exn)
        else:
            self.result.set_exception(exn)


class VolatileSubscription:

    def __init__(
            self, stream, initial_commit, initial_event_number, buffer_size
    ):
        self.last_commit_position = initial_commit
        self.last_event_number = initial_event_number
        self.stream = stream


class PersistentSubscription:

    def __init__(
            self,
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
        self.conversation_id = correlation_id
        self.last_event_number = initial_event_number
        self.stream = stream
        self.buffer_size = buffer_size
        self.auto_ack = auto_ack

    def __str__(self):
        return "Subscription in group %s to %s at event number %d" % (
            self.name, self.stream, self.last_event_number
        )


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

    def error(self, exn) -> Reply:
        if self.state == CreateVolatileSubscription.State.init:
            return Reply(ReplyAction.CompleteError, exn, None)

        return Reply(ReplyAction.RaiseToSubscription, exn, None)

    def start(self):
        msg = proto.SubscribeToStream()
        msg.event_stream_id = self.stream
        msg.resolve_link_tos = self.resolve_links

        return OutboundMessage(
            self.conversation_id, TcpCommand.SubscribeToStream,
            msg.SerializeToString(), self.credential
        )

    def reply_from_init(self, response: InboundMessage):
        self.expect_only(TcpCommand.SubscriptionConfirmation, response)
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
        self.expect_only(TcpCommand.StreamEventAppeared, response)
        result = proto.StreamEventAppeared()
        result.ParseFromString(response.payload)

        return Reply(
            ReplyAction.YieldToSubscription, _make_event(result.event), None
        )

    def drop_subscription(self, response: InboundMessage) -> Reply:
        body = proto.SubscriptionDropped()
        body.ParseFromString(response.payload)

        if self.state == CreateVolatileSubscription.State.init:
            return self.error(
                exceptions.SubscriptionCreationFailed(
                    self.conversation_id, body.reason
                )
            )

        return Reply(ReplyAction.FinishSubscription, None, None)

    def reply(self, response: InboundMessage):

        if response.command == TcpCommand.SubscriptionDropped:
            return self.drop_subscription(response)

        if self.state == CreateVolatileSubscription.State.init:
            return self.reply_from_init(response)

        if self.state == CreateVolatileSubscription.State.live:
            return self.reply_from_live(response)


class CreatePersistentSubscription(MagicConversation):

    def __init__(
            self,
            name,
            stream,
            resolve_links=True,
            start_from=-1,
            timeout_ms=30000,
            record_statistics=False,
            live_buffer_size=500,
            read_batch_size=500,
            buffer_size=1000,
            max_retry_count=10,
            prefer_round_robin=True,
            checkpoint_after_ms=2000,
            checkpoint_max_count=1024,
            checkpoint_min_count=10,
            subscriber_max_count=10,
            credentials=None,
            conversation_id=None,
            consumer_strategy=messages.ROUND_ROBIN
    ) -> None:
        super().__init__(conversation_id, credentials)
        self.stream = stream
        self.name = name
        self.resolve_links = resolve_links
        self.start_from = start_from
        self.timeout_ms = timeout_ms
        self.record_statistics = record_statistics
        self.live_buffer_size = live_buffer_size
        self.read_batch_size = read_batch_size
        self.buffer_size = buffer_size
        self.max_retry_count = max_retry_count
        self.prefer_round_robin = prefer_round_robin
        self.checkpoint_after_time = checkpoint_after_ms
        self.checkpoint_max_count = checkpoint_max_count
        self.checkpoint_min_count = checkpoint_min_count
        self.subscriber_max_count = subscriber_max_count
        self.consumer_strategy = consumer_strategy

    async def start(self, output: Queue) -> None:
        msg = proto.CreatePersistentSubscription()
        msg.subscription_group_name = self.name
        msg.event_stream_id = self.stream
        msg.start_from = self.start_from
        msg.resolve_link_tos = self.resolve_links
        msg.message_timeout_milliseconds = self.timeout_ms
        msg.record_statistics = self.record_statistics
        msg.live_buffer_size = self.live_buffer_size
        msg.read_batch_size = self.read_batch_size
        msg.buffer_size = self.buffer_size
        msg.max_retry_count = self.max_retry_count
        msg.prefer_round_robin = self.prefer_round_robin
        msg.checkpoint_after_time = self.checkpoint_after_time
        msg.checkpoint_max_count = self.checkpoint_max_count
        msg.checkpoint_min_count = self.checkpoint_min_count
        msg.subscriber_max_count = self.subscriber_max_count
        msg.named_consumer_strategy = self.consumer_strategy

        await output.put(
            OutboundMessage(
                self.conversation_id, TcpCommand.CreatePersistentSubscription,
                msg.SerializeToString(), self.credential
            )
        )

    async def reply(self, message: InboundMessage, output: Queue) -> None:
        self.expect_only(
            TcpCommand.CreatePersistentSubscriptionCompleted, message
        )

        result = proto.CreatePersistentSubscriptionCompleted()
        result.ParseFromString(message.payload)

        if result.result == SubscriptionResult.Success:
            self.result.set_result(None)

        elif result.result == SubscriptionResult.AccessDenied:
            await self.error(
                exceptions.AccessDenied(
                    self.conversation_id,
                    type(self).__name__, result.reason
                )
            )
        else:
            await self.error(
                exceptions.SubscriptionCreationFailed(
                    self.conversation_id, result.reason
                )
            )


class ConnectPersistentSubscription(MagicConversation):

    class State(IntEnum):
        init = 0
        catch_up = 1
        live = 2

    def __init__(
            self,
            name,
            stream,
            max_in_flight=10,
            credentials=None,
            conversation_id=None,
            auto_ack=False
    ) -> None:
        super().__init__(conversation_id, credentials)
        self.stream = stream
        self.max_in_flight = max_in_flight
        self.name = name
        self.state = ConnectPersistentSubscription.State.init
        self.auto_ack = auto_ack

    async def start(self, output: Queue) -> None:
        msg = proto.ConnectToPersistentSubscription()
        msg.subscription_id = self.name
        msg.event_stream_id = self.stream
        msg.allowed_in_flight_messages = self.max_in_flight

        await output.put(OutboundMessage(
            self.conversation_id, TcpCommand.ConnectToPersistentSubscription,
            msg.SerializeToString(), self.credential
        ))

    def reply_from_init(self, response: InboundMessage):
        self.expect_only(
            TcpCommand.PersistentSubscriptionConfirmation, response
        )
        result = proto.PersistentSubscriptionConfirmation()
        result.ParseFromString(response.payload)

        self.state = ConnectPersistentSubscription.State.live

        self.result.set_result(
            PersistentSubscription(
                result.subscription_id, self.stream, self.conversation_id,
                result.last_commit_position, result.last_event_number,
                self.max_in_flight, self.auto_ack
            ))

    def reply_from_live(self, response: InboundMessage):
        if response.command == TcpCommand.PersistentSubscriptionConfirmation:
            return Reply(ReplyAction.ContinueSubscription, None, None)

        self.expect_only(
            TcpCommand.PersistentSubscriptionStreamEventAppeared, response
        )
        result = proto.StreamEventAppeared()
        result.ParseFromString(response.payload)

        return Reply(
            ReplyAction.YieldToSubscription, _make_event(result.event), None
        )

    def drop_subscription(self, response: InboundMessage) -> Reply:
        body = proto.SubscriptionDropped()
        body.ParseFromString(response.payload)

        if (self.state == ConnectPersistentSubscription.State.live and
                body.reason == messages.SubscriptionDropReason.Unsubscribed):

            return Reply(ReplyAction.FinishSubscription, None, None)

        if self.state == ConnectPersistentSubscription.State.live:
            return self.error(
                exceptions.SubscriptionFailed(
                    self.conversation_id, body.reason
                )
            )

        return self.error(
            exceptions.SubscriptionCreationFailed(
                self.conversation_id, body.reason
            )
        )

    def error(self, exn) -> Reply:
        logging.error("Becauseages?")
        if self.state == CreateVolatileSubscription.State.init:
            self.result.set_exception(exn)

        return Reply(ReplyAction.RaiseToSubscription, exn, None)

    async def reply(self, message: InboundMessage, output: Queue) -> None:

        if message.command == TcpCommand.SubscriptionDropped:
            self.drop_subscription(message)

        elif self.state == ConnectPersistentSubscription.State.init:
            self.reply_from_init(message)

        elif self.state == ConnectPersistentSubscription.State.live:
            self.reply_from_live(message)
