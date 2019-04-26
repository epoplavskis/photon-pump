import json
import logging
import sys
import time
from asyncio import Future, Queue
from asyncio.base_futures import InvalidStateError
from enum import IntEnum
from typing import NamedTuple, Optional, Sequence, Union
from uuid import UUID, uuid4

from photonpump import exceptions
from photonpump import messages as messages
from photonpump import messages_pb2 as proto
from photonpump.messages import (
    AllStreamSlice,
    ContentType,
    Credential,
    ExpectedVersion,
    InboundMessage,
    NewEvent,
    NotHandledReason,
    OutboundMessage,
    Position,
    ReadAllResult,
    ReadEventResult,
    ReadStreamResult,
    StreamDirection,
    StreamSlice,
    SubscriptionResult,
    TcpCommand,
    _make_event,
)


class StreamingIterator:
    def __init__(self, size=0):
        self.items = Queue(size)
        self.finished = False
        self.fut = None
        self.last_item = None

    def __aiter__(self):
        return self

    async def enqueue_items(self, items):

        for item in items:
            await self.enqueue(item)

    async def enqueue(self, item):
        await self.items.put(item)
        self.last_item = item

    async def anext(self):
        return await self.__anext__()

    async def __anext__(self):

        if self.finished and self.items.empty():
            raise StopAsyncIteration()

        _next = await self.items.get()

        if isinstance(_next, Exception):
            raise _next

        return _next

    async def asend(self, item):
        await self.items.put(item)

    @property
    def last_event_number(self):
        if self.last_item is None:
            return None

        return self.last_item.event_number


class Conversation:
    def __init__(
        self,
        conversation_id: Optional[UUID] = None,
        credential: Optional[Credential] = None,
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
        if not isinstance(other, Conversation):
            return False

        return self.conversation_id == other.conversation_id

    async def start(self, output: Queue) -> Future:
        pass

    async def reply(self, message: InboundMessage, output: Queue) -> None:
        pass

    async def error(self, exn: Exception) -> None:
        self.is_complete = True
        self.result.set_exception(exn)

    def expect_only(self, response: InboundMessage, *commands: TcpCommand):
        if response.command not in commands:
            raise exceptions.UnexpectedCommand(commands, response.command)

    async def respond_to(self, response: InboundMessage, output: Queue) -> None:
        try:
            if response.command is TcpCommand.BadRequest:
                return await self.conversation_error(exceptions.BadRequest, response)

            if response.command is TcpCommand.NotAuthenticated:
                return await self.conversation_error(
                    exceptions.NotAuthenticated, response
                )

            if response.command is TcpCommand.NotHandled:
                return await self.unhandled_message(response)

            return await self.reply(response, output)
        except Exception as exn:
            self._logger.exception("Failed to read server response", exc_info=True)
            exc_info = sys.exc_info()

            return await self.error(
                exceptions.PayloadUnreadable(
                    self.conversation_id, response.payload, exn
                ).with_traceback(exc_info[2])
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
        error = response.payload.decode("UTF-8")
        exn = exn_type(self.conversation_id, error)

        return await self.error(exn)


class TimerConversation(Conversation):
    def __init__(self, conversation_id, credential):
        super().__init__(conversation_id, credential)
        self.started_at = time.perf_counter()

    async def start(self, output: Queue) -> None:
        self.started_at = time.perf_counter()
        logging.debug("TimerConversation started (%s)", self.conversation_id)

    async def reply(self, message: InboundMessage, output: Queue) -> None:
        logging.info("Replying from conversation %s", self)
        responded_at = time.perf_counter()
        self.result.set_result(responded_at - self.started_at)
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
                self.conversation_id, cmd, b"", self.credential, one_way=one_way
            )
        )
        logging.debug("Heartbeat started (%s)", self.conversation_id)

    async def reply(self, message: InboundMessage, output: Queue) -> None:
        self.expect_only(message, TcpCommand.HeartbeatResponse)
        await super().reply(message, output)


class Ping(TimerConversation):
    def __init__(self, conversation_id: UUID = None, credential=None) -> None:
        super().__init__(conversation_id or uuid4(), credential)

    async def start(self, output: Queue) -> Future:
        await super().start(output)

        if output:
            await output.put(
                OutboundMessage(
                    self.conversation_id, TcpCommand.Ping, b"", self.credential
                )
            )
        logging.debug("Ping started (%s)", self.conversation_id)

        return self.result

    async def reply(self, message: InboundMessage, output: Queue) -> None:
        self.expect_only(message, TcpCommand.Pong)
        await super().reply(message, output)


class WriteEvents(Conversation):
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
        loop=None,
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
                e.data = event.data.encode("UTF-8")
            elif isinstance(event.data, bytes):
                e.data_content_type = ContentType.Binary
                e.data = event.data
            elif event.data:
                e.data_content_type = ContentType.Json
                e.data = json.dumps(event.data).encode("UTF-8")
            else:
                e.data_content_type = ContentType.Binary
                e.data = bytes()

            if event.metadata:
                e.metadata_content_type = ContentType.Json
                e.metadata = json.dumps(event.metadata).encode("UTF-8")
            else:
                e.metadata_content_type = ContentType.Binary
                e.metadata = bytes()

        data = msg.SerializeToString()

        await output.put(
            OutboundMessage(
                self.conversation_id, TcpCommand.WriteEvents, data, self.credential
            )
        )
        logging.debug(
            "WriteEvents started on %s (%s)", self.stream, self.conversation_id
        )

    async def reply(self, message: InboundMessage, output: Queue) -> None:
        self.expect_only(message, TcpCommand.WriteEventsCompleted)
        result = proto.WriteEventsCompleted()
        result.ParseFromString(message.payload)
        try:
            self.result.set_result(result)
            self.is_complete = True
        except InvalidStateError as exn:
            logging.error(self.result, message, self, exc_info=True)
            raise exn


class ReadAllEventsBehaviour:
    async def reply(self, message: InboundMessage, output: Queue):
        result = proto.ReadAllEventsCompleted()
        result.ParseFromString(message.payload)

        if result.result == ReadAllResult.Success:
            await self.success(result, output)
        elif result.result == ReadAllResult.Error:
            await self.error(
                exceptions.ReadError(self.conversation_id, "$all", result.error)
            )
        elif result.result == ReadAllResult.AccessDenied:
            await self.error(
                exceptions.AccessDenied(
                    self.conversation_id, type(self).__name__, result.error
                )
            )


class ReadStreamEventsBehaviour:
    def __init__(self, result_type, response_cls):
        self.result_type = result_type
        self.response_cls = response_cls

    def success(self, result, output: Queue):
        pass

    async def reply(self, message: InboundMessage, output: Queue):
        result = self.response_cls()
        result.ParseFromString(message.payload)

        if result.result == self.result_type.Success:
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
                exceptions.ReadError(self.conversation_id, self.stream, result.error)
            )
        elif result.result == self.result_type.AccessDenied:
            await self.error(
                exceptions.AccessDenied(
                    self.conversation_id,
                    type(self).__name__,
                    result.error,
                    stream=self.stream,
                )
            )
        elif (
            self.result_type == ReadEventResult
            and result.result == self.result_type.NotFound
        ):
            await self.error(
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
        conversation_id: Optional[UUID] = None,
        credentials=None,
    ) -> None:

        Conversation.__init__(self, conversation_id, credential=credentials)
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

        await output.put(
            OutboundMessage(
                self.conversation_id, TcpCommand.Read, data, self.credential
            )
        )
        logging.debug("ReadEvent started on %s (%s)", self.stream, self.conversation_id)

    async def success(self, response, output: Queue):
        self.is_complete = True
        self.result.set_result(_make_event(response.event))


class PageStreamEventsBehaviour(Conversation):
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
        msg.resolve_link_tos = self.resolve_link_tos

        data = msg.SerializeToString()

        return OutboundMessage(self.conversation_id, command, data, self.credential)

    async def start(self, output):
        await output.put(self._fetch_page_message(self.from_event))
        logging.debug("PageStreamEventsBehaviour started (%s)", self.conversation_id)


class ReadAllEvents(ReadAllEventsBehaviour, Conversation):
    """Command class for reading all events from a stream.

    Args:
        commit_position: The commit_position.
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
        from_position: Optional[Position] = None,
        max_count: int = 100,
        resolve_links: bool = True,
        require_master: bool = False,
        direction: StreamDirection = StreamDirection.Forward,
        credentials=None,
        conversation_id: UUID = None,
    ) -> None:

        Conversation.__init__(self, conversation_id, credential=credentials)
        ReadStreamEventsBehaviour.__init__(
            self, ReadAllResult, proto.ReadAllEventsCompleted
        )
        self.has_first_page = False
        self.direction = direction
        self.from_position = from_position
        self.max_count = max_count
        self.require_master = require_master
        self.resolve_link_tos = resolve_links

    async def success(self, result: proto.ReadAllEventsCompleted, output: Queue):
        events = [_make_event(x) for x in result.events]

        self.is_complete = True
        self.result.set_result(
            AllStreamSlice(
                events,
                Position(result.next_commit_position, result.next_prepare_position),
                Position(result.commit_position, result.prepare_position),
            )
        )

    def _fetch_page_message(self, from_position: Position):
        self._logger.debug(
            "Requesting page of %d events from %s", self.max_count, from_position
        )

        if self.direction == StreamDirection.Forward:
            command = TcpCommand.ReadAllEventsForward
        else:
            command = TcpCommand.ReadAllEventsBackward

        msg = proto.ReadAllEvents()
        msg.commit_position = from_position.commit
        msg.prepare_position = from_position.prepare
        msg.max_count = self.max_count
        msg.require_master = self.require_master
        msg.resolve_link_tos = self.resolve_link_tos

        data = msg.SerializeToString()

        return OutboundMessage(self.conversation_id, command, data, self.credential)

    async def start(self, output):
        await output.put(self._fetch_page_message(self.from_position))


class ReadStreamEvents(ReadStreamEventsBehaviour, PageStreamEventsBehaviour):
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
        conversation_id: UUID = None,
    ) -> None:

        Conversation.__init__(self, conversation_id, credential=credentials)
        ReadStreamEventsBehaviour.__init__(
            self, ReadStreamResult, proto.ReadStreamEventsCompleted
        )
        self.has_first_page = False
        self.stream = stream
        self.direction = direction
        self.from_event = from_event
        self.batch_size = max_count
        self.require_master = require_master
        self.resolve_link_tos = resolve_links

    async def success(self, result: proto.ReadStreamEventsCompleted, output: Queue):
        events = [_make_event(x) for x in result.events]

        self.is_complete = True
        self.result.set_result(
            StreamSlice(
                events,
                result.next_event_number,
                result.last_event_number,
                None,
                result.last_commit_position,
                result.is_end_of_stream,
            )
        )

    def _fetch_page_message(self, from_event):
        self._logger.debug(
            "Requesting page of %d events from number %d",
            self.batch_size,
            self.from_event,
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
        msg.resolve_link_tos = self.resolve_link_tos

        data = msg.SerializeToString()

        return OutboundMessage(self.conversation_id, command, data, self.credential)


class IterAllEvents(ReadAllEventsBehaviour, Conversation):
    """
    Command class for iterating all events in the database.

    Args:
        from_position (optional): The position to start reading from.
          Defaults to photonpump.Beginning when direction is Forward,
          photonpump.End when direction is Backward.
        batch_size (optional): The maximum number of events to read at a time.
        resolve_links (optional): True if eventstore should
            automatically resolve Link Events, otherwise False.
        require_master (optional): True if this command must be
            sent direct to the master node, otherwise False.
        direction (optional): Controls whether to read forward or backward
          through the events. Defaults to  StreamDirection.Forward
        correlation_id (optional): A unique identifer for this
            command.
    """

    def __init__(
        self,
        from_position: Position = None,
        batch_size: int = 100,
        resolve_links: bool = True,
        require_master: bool = False,
        direction: StreamDirection = StreamDirection.Forward,
        credentials=None,
        conversation_id: UUID = None,
    ):

        Conversation.__init__(self, conversation_id, credentials)
        self.batch_size = batch_size
        self.has_first_page = False
        self.resolve_link_tos = resolve_links
        self.require_master = require_master
        self.from_position = from_position or Position(0, 0)
        self.direction = direction
        self._logger = logging.get_named_logger(IterAllEvents)
        self.iterator = StreamingIterator(self.batch_size)

        if direction == StreamDirection.Forward:
            self.command = TcpCommand.ReadAllEventsForward
        else:
            self.command = TcpCommand.ReadAllEventsBackward

    def _fetch_page_message(self, from_position):
        if self.direction == StreamDirection.Forward:
            command = TcpCommand.ReadAllEventsForward
        else:
            command = TcpCommand.ReadAllEventsBackward

        msg = proto.ReadAllEvents()
        msg.commit_position = from_position.commit
        msg.prepare_position = from_position.prepare
        msg.max_count = self.batch_size
        msg.resolve_link_tos = self.resolve_link_tos
        msg.require_master = self.require_master

        data = msg.SerializeToString()

        return OutboundMessage(self.conversation_id, command, data, self.credential)

    async def start(self, output):
        await output.put(self._fetch_page_message(self.from_position))
        logging.debug("IterAllEvents started (%s)", self.conversation_id)

    async def success(self, result: proto.ReadAllEventsCompleted, output: Queue):
        if not self.has_first_page:
            self.result.set_result(self.iterator)
            self.has_first_page = True

        events = [_make_event(x) for x in result.events]
        await self.iterator.enqueue_items(events)

        at_end = result.commit_position == result.next_commit_position

        if at_end:
            self.is_complete = True
            await self.iterator.asend(StopAsyncIteration())

            return

        await output.put(
            self._fetch_page_message(
                Position(result.next_commit_position, result.next_prepare_position)
            )
        )

    async def error(self, exn: Exception) -> None:
        self.is_complete = True

        if self.has_first_page:
            await self.iterator.asend(exn)
        else:
            self.result.set_exception(exn)


class IterStreamEvents(ReadStreamEventsBehaviour, PageStreamEventsBehaviour):
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
        conversation_id: UUID = None,
    ):

        Conversation.__init__(self, conversation_id, credentials)
        ReadStreamEventsBehaviour.__init__(
            self, ReadStreamResult, proto.ReadStreamEventsCompleted
        )
        self.batch_size = batch_size
        self.has_first_page = False
        self.stream = stream
        self.resolve_link_tos = resolve_links
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

    async def start(self, output: Queue):
        await output.put(
            self._fetch_page_message(self.iterator.last_event_number or self.from_event)
        )
        logging.debug(
            "IterStreamEvents started on %s (%s)", self.stream, self.conversation_id
        )

    async def success(self, result: proto.ReadStreamEventsCompleted, output: Queue):

        if not result.is_end_of_stream:
            await output.put(self._fetch_page_message(result.next_event_number))

        events = [_make_event(x) for x in result.events]
        await self.iterator.enqueue_items(events)

        if not self.has_first_page:
            self.result.set_result(self.iterator)
            self.has_first_page = True

        if result.is_end_of_stream:
            self.is_complete = True
            await self.iterator.asend(StopAsyncIteration())

    async def error(self, exn: Exception) -> None:
        self.is_complete = True

        if self.has_first_page:
            await self.iterator.asend(exn)
        else:
            self.result.set_exception(exn)


class PersistentSubscription:
    def __init__(
        self,
        name,
        stream,
        correlation_id,
        initial_commit,
        initial_event_number,
        buffer_size,
        out_queue,
        auto_ack=False,
    ):
        self.initial_commit_position = initial_commit
        self.name = name
        self.conversation_id = correlation_id
        self.last_event_number = initial_event_number
        self.stream = stream
        self.buffer_size = buffer_size
        self.auto_ack = auto_ack
        self.events = StreamingIterator()
        self.out_queue = out_queue

    def __str__(self):
        return "Subscription in group %s to %s at event number %d" % (
            self.name,
            self.stream,
            self.last_event_number,
        )

    async def ack(self, event):
        payload = proto.PersistentSubscriptionAckEvents()
        payload.subscription_id = self.name
        payload.processed_event_ids.append(event.received_event.id.bytes_le)
        message = OutboundMessage(
            self.conversation_id,
            TcpCommand.PersistentSubscriptionAckEvents,
            payload.SerializeToString(),
        )

        await self.out_queue.put(message)


class CreatePersistentSubscription(Conversation):
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
        consumer_strategy=messages.ROUND_ROBIN,
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
                self.conversation_id,
                TcpCommand.CreatePersistentSubscription,
                msg.SerializeToString(),
                self.credential,
            )
        )

        logging.debug(
            "CreatePersistentSubscription started on %s (%s)",
            self.stream,
            self.conversation_id,
        )

    async def reply(self, message: InboundMessage, output: Queue) -> None:
        self.expect_only(message, TcpCommand.CreatePersistentSubscriptionCompleted)

        result = proto.CreatePersistentSubscriptionCompleted()
        result.ParseFromString(message.payload)

        if result.result == SubscriptionResult.Success:
            self.is_complete = True
            self.result.set_result(None)

        elif result.result == SubscriptionResult.AccessDenied:
            await self.error(
                exceptions.AccessDenied(
                    self.conversation_id, type(self).__name__, result.reason
                )
            )
        else:
            await self.error(
                exceptions.SubscriptionCreationFailed(
                    self.conversation_id, result.reason
                )
            )


class ConnectPersistentSubscription(Conversation):
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
        auto_ack=False,
    ) -> None:
        super().__init__(conversation_id, credentials)
        self.stream = stream
        self.max_in_flight = max_in_flight
        self.name = name
        self.is_live = False
        self.auto_ack = auto_ack

    async def start(self, output: Queue) -> None:
        msg = proto.ConnectToPersistentSubscription()
        msg.subscription_id = self.name
        msg.event_stream_id = self.stream
        msg.allowed_in_flight_messages = self.max_in_flight

        await output.put(
            OutboundMessage(
                self.conversation_id,
                TcpCommand.ConnectToPersistentSubscription,
                msg.SerializeToString(),
                self.credential,
            )
        )
        logging.debug(
            "ConnectPersistentSubscription started on %s (%s)",
            self.stream,
            self.conversation_id,
        )

    def reply_from_init(self, response: InboundMessage, output: Queue):
        self.expect_only(response, TcpCommand.PersistentSubscriptionConfirmation)
        result = proto.PersistentSubscriptionConfirmation()
        result.ParseFromString(response.payload)

        self.subscription = PersistentSubscription(
            result.subscription_id,
            self.stream,
            self.conversation_id,
            result.last_commit_position,
            result.last_event_number,
            self.max_in_flight,
            output,
            self.auto_ack,
        )

        self.is_live = True
        self.result.set_result(self.subscription)

    async def reply_from_live(self, response: InboundMessage, output: Queue):
        if response.command == TcpCommand.PersistentSubscriptionConfirmation:
            self.subscription.out_queue = output

            return

        self.expect_only(response, TcpCommand.PersistentSubscriptionStreamEventAppeared)
        result = proto.StreamEventAppeared()
        result.ParseFromString(response.payload)
        await self.subscription.events.enqueue(_make_event(result.event))

    async def drop_subscription(self, response: InboundMessage) -> None:
        body = proto.SubscriptionDropped()
        body.ParseFromString(response.payload)

        if self.is_live and body.reason == messages.SubscriptionDropReason.Unsubscribed:

            await self.subscription.events.enqueue(StopAsyncIteration())

            return

        if self.is_live:
            await self.error(
                exceptions.SubscriptionFailed(self.conversation_id, body.reason)
            )

            return

        await self.error(
            exceptions.SubscriptionCreationFailed(self.conversation_id, body.reason)
        )

    async def error(self, exn) -> None:
        if self.is_live:
            await self.subscription.events.asend(exn)
        else:
            self.result.set_exception(exn)

    async def reply(self, message: InboundMessage, output: Queue) -> None:

        if message.command == TcpCommand.SubscriptionDropped:
            await self.drop_subscription(message)

        elif self.is_live:
            await self.reply_from_live(message, output)

        else:
            self.reply_from_init(message, output)


class SubscribeToStream(Conversation):
    def __init__(
        self, stream, resolve_link_tos=True, conversation_id=None, credentials=None
    ):
        self.stream = stream
        self.resolve_link_tos = resolve_link_tos
        self.is_live = False
        super().__init__(conversation_id, credentials)

    async def start(self, output: Queue) -> None:
        msg = proto.SubscribeToStream()
        msg.event_stream_id = self.stream
        msg.resolve_link_tos = self.resolve_link_tos

        await output.put(
            OutboundMessage(
                self.conversation_id,
                TcpCommand.SubscribeToStream,
                msg.SerializeToString(),
                self.credential,
            )
        )
        logging.debug(
            "SubscribeToStream started on %s (%s)", self.stream, self.conversation_id
        )

    async def drop_subscription(self, response: InboundMessage) -> None:
        body = proto.SubscriptionDropped()
        body.ParseFromString(response.payload)

        if self.is_live and body.reason == messages.SubscriptionDropReason.Unsubscribed:

            await self.subscription.events.enqueue(StopAsyncIteration())

            return

        if self.is_live:
            await self.error(
                exceptions.SubscriptionFailed(self.conversation_id, body.reason)
            )

            return

        await self.error(
            exceptions.SubscriptionCreationFailed(self.conversation_id, body.reason)
        )

    async def error(self, exn) -> None:
        if self.is_live:
            await self.subscription.raise_error(exn)
        else:
            self.result.set_exception(exn)

    async def reply_from_init(self, message: InboundMessage, output: Queue):
        self.expect_only(message, TcpCommand.SubscriptionConfirmation)

        result = proto.SubscriptionConfirmation()
        result.ParseFromString(message.payload)

        self.subscription = VolatileSubscription(
            self.conversation_id,
            self.stream,
            output,
            result.last_event_number,
            result.last_commit_position,
        )

        self.is_live = True
        self.result.set_result(self.subscription)

    async def reply_from_live(self, message: InboundMessage) -> None:
        self.expect_only(
            message, TcpCommand.StreamEventAppeared, TcpCommand.SubscriptionConfirmation
        )

        if message.command is TcpCommand.SubscriptionConfirmation:
            return

        result = proto.StreamEventAppeared()
        result.ParseFromString(message.payload)

        await self.subscription.events.enqueue(_make_event(result.event))

    async def reply(self, message: InboundMessage, output: Queue) -> None:
        if message.command == TcpCommand.SubscriptionDropped:
            await self.drop_subscription(message)
        elif self.is_live:
            await self.reply_from_live(message)
        else:
            await self.reply_from_init(message, output)


class CatchupSubscriptionPhase(IntEnum):

    READ_HISTORICAL = 0
    CATCH_UP = 1
    LIVE = 2
    RECONNECT = 3


class VolatileSubscription:
    def __init__(
        self,
        conversation_id,
        stream,
        queue,
        event_number,
        commit_position,
        iterator=None,
    ):
        self.stream = stream
        self.output_queue = queue
        self.id = conversation_id
        self.first_event_number = event_number
        self.first_commit_position = commit_position
        self.last_event_number = event_number
        self.last_commit_position = commit_position
        self.events = iterator or StreamingIterator()
        self.is_complete = False

    async def unsubscribe(self):
        await self.output_queue.put(
            messages.OutboundMessage(self.id, TcpCommand.UnsubscribeFromStream, bytes())
        )

    async def raise_error(self, exn: Exception) -> None:
        self.is_complete = True
        await self.events.asend(exn)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if not self.is_complete:
            await self.unsubscribe()


class CatchupSubscription(ReadStreamEventsBehaviour, PageStreamEventsBehaviour):
    def __init__(
        self,
        stream,
        start_from=0,
        batch_size=100,
        credential=None,
        conversation_id=None,
    ):
        self.stream = stream
        self.iterator = StreamingIterator()
        self.conversation_id = conversation_id or uuid4()
        self._logger = logging.get_named_logger(
            CatchupSubscription, self.conversation_id
        )
        self.from_event = start_from
        self.direction = StreamDirection.Forward
        self.batch_size = batch_size
        self.has_first_page = False
        self.require_master = False
        self.resolve_link_tos = True
        self.credential = credential
        self.result = Future()
        self.phase = CatchupSubscriptionPhase.READ_HISTORICAL
        self.buffer = []
        self.subscribe_from = -1
        self.next_event_number = self.from_event
        self.last_event_number = -1
        Conversation.__init__(self, conversation_id, credential)
        ReadStreamEventsBehaviour.__init__(
            self, ReadStreamResult, proto.ReadStreamEventsCompleted
        )

    @property
    def is_live(self):
        return self.phase == CatchupSubscriptionPhase.LIVE

    async def error(self, exn) -> None:
        if self.result.done():
            await self.subscription.raise_error(exn)
        else:
            self.result.set_exception(exn)

    async def reconnect(self, output: Queue) -> None:
        self.phase = CatchupSubscriptionPhase.RECONNECT
        self.buffer = []
        await self.subscription.unsubscribe()

    async def start(self, output):
        if self.phase > CatchupSubscriptionPhase.READ_HISTORICAL:
            self._logger.info("Tear down previous subscription")
            await self.reconnect(output)

            return

        self._logger.info("Starting catchup subscription at %s", self.from_event)
        self.from_event = max(
            self.from_event, self.next_event_number, self.last_event_number
        )
        await PageStreamEventsBehaviour.start(self, output)
        logging.debug(
            "CatchupSubscription started on %s (%s)", self.stream, self.conversation_id
        )

    async def drop_subscription(self, response: InboundMessage) -> None:
        body = proto.SubscriptionDropped()
        body.ParseFromString(response.payload)

        if body.reason == messages.SubscriptionDropReason.Unsubscribed:

            await self.subscription.events.enqueue(StopAsyncIteration())

            return

        if self.result.done():
            await self.error(
                exceptions.SubscriptionFailed(self.conversation_id, body.reason)
            )

            return

        await self.error(
            exceptions.SubscriptionCreationFailed(self.conversation_id, body.reason)
        )

    async def _yield_events(self, events):
        for event in events:
            if event.event_number <= self.last_event_number:
                continue
            await self.iterator.asend(event)
            self.last_event_number = event.event_number

    async def _move_to_next_phase(self, output):
        if self.phase == CatchupSubscriptionPhase.READ_HISTORICAL:
            self.phase = CatchupSubscriptionPhase.CATCH_UP
            self._logger.info(
                "Caught up with historical events, creating volatile subscription"
            )
            await self._subscribe(output)
        elif self.phase == CatchupSubscriptionPhase.CATCH_UP:
            self.phase = CatchupSubscriptionPhase.LIVE
            await self._yield_events(self.buffer)

    async def reply_from_live(self, message, output):
        if message.command == TcpCommand.SubscriptionDropped:
            await self.drop_subscription(message)

            return

        self.expect_only(message, TcpCommand.StreamEventAppeared)
        result = proto.StreamEventAppeared()
        result.ParseFromString(message.payload)

        await self._yield_events([_make_event(result.event)])

    async def reply_from_catch_up(self, message, output):
        if message.command == TcpCommand.SubscriptionDropped:
            await self.drop_subscription(message)
        elif message.command == TcpCommand.SubscriptionConfirmation:
            confirmation = proto.SubscriptionConfirmation()
            confirmation.ParseFromString(message.payload)
            self.subscribe_from = confirmation.last_event_number
            self._logger.info(
                "Subscribed successfully, catching up with missed events from %s",
                self.next_event_number,
            )
            await output.put(self._fetch_page_message(self.next_event_number))
        elif message.command == TcpCommand.StreamEventAppeared:
            result = proto.StreamEventAppeared()
            result.ParseFromString(message.payload)
            self.buffer.append(_make_event(result.event))
        else:
            await ReadStreamEventsBehaviour.reply(self, message, output)

    async def reply_from_reconnect(self, message: InboundMessage, output: Queue):
        if message.command != TcpCommand.SubscriptionDropped:
            return
        self.phase = CatchupSubscriptionPhase.READ_HISTORICAL
        await self.start(output)

    async def reply(self, message: InboundMessage, output: Queue):

        if self.phase == CatchupSubscriptionPhase.READ_HISTORICAL:
            self.expect_only(message, TcpCommand.ReadStreamEventsForwardCompleted)
            await ReadStreamEventsBehaviour.reply(self, message, output)
        elif self.phase == CatchupSubscriptionPhase.CATCH_UP:
            await self.reply_from_catch_up(message, output)
        elif self.phase == CatchupSubscriptionPhase.RECONNECT:
            await self.reply_from_reconnect(message, output)
        else:
            await self.reply_from_live(message, output)

    async def success(self, result: proto.ReadStreamEventsCompleted, output: Queue):

        finished = False
        events = []

        for e in result.events:
            event = _make_event(e)
            events.append(event)
        await self._yield_events(events)

        self.next_event_number = result.next_event_number

        # Todo: we should finish if the next event > subscription_start_pos

        if result.is_end_of_stream:
            finished = True

        if not self.has_first_page:
            self.subscription = VolatileSubscription(
                self.conversation_id, self.stream, output, 0, 0, self.iterator
            )
            self.result.set_result(self.subscription)
            self.has_first_page = True

        if finished:
            await self._move_to_next_phase(output)
        else:
            await output.put(self._fetch_page_message(result.next_event_number))

    async def _subscribe(self, output: Queue) -> None:
        msg = proto.SubscribeToStream()
        msg.event_stream_id = self.stream
        msg.resolve_link_tos = self.resolve_link_tos

        await output.put(
            OutboundMessage(
                self.conversation_id,
                TcpCommand.SubscribeToStream,
                msg.SerializeToString(),
                self.credential,
            )
        )


class CatchupAllSubscription(ReadAllEventsBehaviour, Conversation):
    def __init__(
        self, start_from=None, batch_size=100, credential=None, conversation_id=None
    ):
        self.iterator = StreamingIterator()
        self.conversation_id = conversation_id or uuid4()
        self._logger = logging.get_named_logger(
            CatchupAllSubscription, self.conversation_id
        )
        self.from_position = start_from or Position(0, 0)
        self.direction = StreamDirection.Forward
        self.batch_size = batch_size
        self.has_first_page = False
        self.require_master = False
        self.resolve_link_tos = True
        self.credential = credential
        self.result = Future()
        self.phase = CatchupSubscriptionPhase.READ_HISTORICAL
        self.buffer = []
        self.next_position = self.from_position
        self.last_position = Position.min
        Conversation.__init__(self, conversation_id, credential)
        ReadAllEventsBehaviour.__init__(self)

    async def _yield_events(self, events):
        for event in events:
            print(
                event.position, self.last_position, event.position > self.last_position
            )
            if event.position > self.last_position:
                await self.iterator.enqueue(event)
                self.last_position = event.position

    async def start(self, output):
        if self.phase > CatchupSubscriptionPhase.READ_HISTORICAL:
            self._logger.info("Tear down previous subscription")
            await self.reconnect(output)

            return

        self.from_position = max(self.from_position, self.last_position)

        self._logger.info("Starting catchup subscription at %s", self.from_position)
        if self.direction == StreamDirection.Forward:
            command = TcpCommand.ReadAllEventsForward
        else:
            command = TcpCommand.ReadAllEventsBackward

        msg = proto.ReadAllEvents()
        msg.commit_position = self.from_position.commit
        msg.prepare_position = self.from_position.prepare
        msg.max_count = self.batch_size
        msg.resolve_link_tos = self.resolve_link_tos
        msg.require_master = self.require_master

        data = msg.SerializeToString()

        await output.put(
            OutboundMessage(self.conversation_id, command, data, self.credential)
        )

        logging.debug("CatchupAllSubscription started (%s)", self.conversation_id)

    @property
    def is_live(self):
        return self.phase == CatchupSubscriptionPhase.LIVE

    async def error(self, exn) -> None:
        if self.result.done():
            await self.subscription.raise_error(exn)
        else:
            self.result.set_exception(exn)

    async def reconnect(self, output: Queue) -> None:
        self.phase = CatchupSubscriptionPhase.RECONNECT
        self.buffer = []
        await self.subscription.unsubscribe()

    async def drop_subscription(self, response: InboundMessage) -> None:
        body = proto.SubscriptionDropped()
        body.ParseFromString(response.payload)

        if body.reason == messages.SubscriptionDropReason.Unsubscribed:

            await self.subscription.events.enqueue(StopAsyncIteration())

            return

        if self.result.done():
            await self.error(
                exceptions.SubscriptionFailed(self.conversation_id, body.reason)
            )

            return

        await self.error(
            exceptions.SubscriptionCreationFailed(self.conversation_id, body.reason)
        )

    async def _move_to_next_phase(self, output):
        if self.phase == CatchupSubscriptionPhase.READ_HISTORICAL:
            self.phase = CatchupSubscriptionPhase.CATCH_UP
            self._logger.info(
                "Caught up with historical events, creating volatile subscription"
            )
            await self._subscribe(output)
        elif self.phase == CatchupSubscriptionPhase.CATCH_UP:
            self.phase = CatchupSubscriptionPhase.LIVE
            await self._yield_events(self.buffer)

    async def reply_from_live(self, message, output):
        if message.command == TcpCommand.SubscriptionDropped:
            await self.drop_subscription(message)

            return

        self.expect_only(message, TcpCommand.StreamEventAppeared)
        result = proto.StreamEventAppeared()
        result.ParseFromString(message.payload)

        await self._yield_events([_make_event(result.event)])

    async def reply_from_catch_up(self, message, output):
        if message.command == TcpCommand.SubscriptionDropped:
            await self.drop_subscription(message)
        elif message.command == TcpCommand.SubscriptionConfirmation:
            confirmation = proto.SubscriptionConfirmation()
            confirmation.ParseFromString(message.payload)
            self._logger.info(
                "Subscribed successfully, catching up with missed events from %s",
                self.from_position,
            )
            await output.put(self._fetch_page_message(self.from_position))
        elif message.command == TcpCommand.StreamEventAppeared:
            result = proto.StreamEventAppeared()
            result.ParseFromString(message.payload)
            self.buffer.append(_make_event(result.event))
        else:
            await ReadAllEventsBehaviour.reply(self, message, output)

    async def reply_from_reconnect(self, message: InboundMessage, output: Queue):
        if message.command != TcpCommand.SubscriptionDropped:
            return
        self.phase = CatchupSubscriptionPhase.READ_HISTORICAL
        await self.start(output)

    async def reply(self, message: InboundMessage, output: Queue):

        if self.phase == CatchupSubscriptionPhase.READ_HISTORICAL:
            self.expect_only(message, TcpCommand.ReadAllEventsForwardCompleted)
            await ReadAllEventsBehaviour.reply(self, message, output)
        elif self.phase == CatchupSubscriptionPhase.CATCH_UP:
            await self.reply_from_catch_up(message, output)
        elif self.phase == CatchupSubscriptionPhase.RECONNECT:
            await self.reply_from_reconnect(message, output)
        else:
            await self.reply_from_live(message, output)

    async def success(self, result: proto.ReadStreamEventsCompleted, output: Queue):

        finished = result.commit_position == result.next_commit_position
        events = []

        for e in result.events:
            event = _make_event(e)
            events.append(event)

        # Todo: we should finish if the next event > subscription_start_pos

        if not self.has_first_page:
            self.subscription = VolatileSubscription(
                self.conversation_id, "$all", output, 0, 0, self.iterator
            )
            self.result.set_result(self.subscription)
            self.has_first_page = True

        await self._yield_events(events)

        self.from_position = Position(
            result.next_commit_position, result.next_prepare_position
        )
        if finished:
            await self._move_to_next_phase(output)
        else:
            await output.put(self._fetch_page_message(self.from_position))

    async def _subscribe(self, output: Queue) -> None:
        msg = proto.SubscribeToStream()
        msg.event_stream_id = ""
        msg.resolve_link_tos = self.resolve_link_tos

        await output.put(
            OutboundMessage(
                self.conversation_id,
                TcpCommand.SubscribeToStream,
                msg.SerializeToString(),
                self.credential,
            )
        )

    def _fetch_page_message(self, from_position: Position):
        self._logger.debug(
            "Requesting page of %d events from %s", self.batch_size, from_position
        )

        if self.direction == StreamDirection.Forward:
            command = TcpCommand.ReadAllEventsForward
        else:
            command = TcpCommand.ReadAllEventsBackward

        msg = proto.ReadAllEvents()
        msg.commit_position = from_position.commit
        msg.prepare_position = from_position.prepare
        msg.max_count = self.batch_size
        msg.require_master = self.require_master
        msg.resolve_link_tos = self.resolve_link_tos

        data = msg.SerializeToString()

        return OutboundMessage(self.conversation_id, command, data, self.credential)
