import array
import asyncio
import enum
import logging
import struct
import uuid
from typing import Any, NamedTuple, Optional, Sequence

from . import conversations as convo
from . import messages as msg
from . import messages_pb2 as proto
from .discovery import DiscoveryRetryPolicy, NodeService, get_discoverer

HEADER_LENGTH = 1 + 1 + 16
SIZE_UINT_32 = 4


class Event(list):

    def __call__(self, *args, **kwargs):
        for f in self:
            f(*args, **kwargs)

    def __repr__(self):
        return 'Event(%s)' % list.__repr__(self)


class ConnectorCommand(enum.IntEnum):
    Connect = 0
    HandleConnectFailure = 1
    HandleConnectionOpened = 2
    HandleConnectionClosed = 3
    HandleConnectionFailed = 4

    HandleHeartbeatFailed = 5
    HandleHeartbeatSuccess = 6

    HandleConnectorFailed = -2

    Stop = -1


class ConnectorState(enum.IntEnum):
    Begin = 0
    Connecting = 1
    Connected = 2
    Stopping = 3
    Stopped = 4


class ConnectorInstruction(NamedTuple):
    command: ConnectorCommand
    future: Optional[asyncio.Future]
    data: Optional[Any]


class Connector(asyncio.streams.FlowControlMixin):

    def __init__(
            self, discovery, retry_policy=None, ctrl_queue=None, loop=None
    ):
        self.loop = loop or asyncio.get_event_loop()
        self.discovery = discovery
        self.connected = Event()
        self.disconnected = Event()
        self.stopped = Event()
        self.ctrl_queue = ctrl_queue or asyncio.Queue(loop=self.loop)
        self.log = logging.getLogger("photonpump.connection.Connector")
        self._run_loop = asyncio.ensure_future(self._run())
        self.reader = None
        self.writer = None
        self.transport = None
        self.heartbeat_failures = 0
        self.retry_policy = retry_policy or DiscoveryRetryPolicy(
            retries_per_node=0
        )
        self.target_node = None

        self._connection_lost = False
        self._paused = False
        self.state = ConnectorState.Begin
        self.connect_timeout = 5

    def _put_msg(self, msg):
        asyncio.ensure_future(self.ctrl_queue.put(msg))

    def connection_made(self, transport):
        self._put_msg(
            ConnectorInstruction(
                ConnectorCommand.HandleConnectionOpened, None, transport
            )
        )

    def heartbeat_received(self, conversation_id):
        self.retry_policy.record_success(self.target_node)
        self._put_msg(
            ConnectorInstruction(
                ConnectorCommand.HandleHeartbeatSuccess, None, conversation_id
            )
        )

    def data_received(self, data):
        self.reader.feed_data(data)

    def eof_received(self):
        self.log.info("EOF received, tearing down connection")

    def connection_lost(self, exn=None):
        self.log.info('connection_lost {}'.format(exn))
        self.retry_policy.record_failure(self.target_node)

        if exn:
            self._put_msg(
                ConnectorInstruction(
                    ConnectorCommand.HandleConnectionFailed, None, exn
                )
            )
        else:
            self._put_msg(
                ConnectorInstruction(
                    ConnectorCommand.HandleConnectionClosed, None, None
                )
            )

    def heartbeat_failed(self, exn=None):
        self._put_msg(
            ConnectorInstruction(
                ConnectorCommand.HandleHeartbeatFailed, None, exn
            )
        )

    async def _drain_helper(self):
        pass

    async def start(self, target: Optional[NodeService] = None):
        self.state = ConnectorState.Connecting
        await self.ctrl_queue.put(
            ConnectorInstruction(ConnectorCommand.Connect, None, target)
        )

    async def stop(self, exn=None):
        self.log.info("Stopping connector")
        self.state = ConnectorState.Stopping
        future = asyncio.Future()
        await self.ctrl_queue.put(
            ConnectorInstruction(ConnectorCommand.Stop, future, exn)
        )

        return future

    async def reconnect(self):
        self.target_node = None

        if self.transport:
            self.transport.close()
        else:
            await self.start()

    async def _attempt_connect(self, node):
        if not node:
            try:
                self.log.debug("Performing node discovery")
                node = self.target_node = await self.discovery.discover()
            except Exception as e:
                await self.ctrl_queue.put(
                    ConnectorInstruction(
                        ConnectorCommand.HandleConnectorFailed, None, e
                    )
                )

                return
        self.log.info("Connecting to %s:%s", node.address, node.port)
        try:
            await asyncio.wait_for(
                self.loop.create_connection(
                    lambda: self, node.address, node.port
                ), self.connect_timeout
            )
        except Exception as e:
            await self.ctrl_queue.put(
                ConnectorInstruction(
                    ConnectorCommand.HandleConnectFailure, None, e
                )
            )

    async def _on_transport_received(self, transport):
        self.log.info(
            "PhotonPump is connected to eventstore instance at %s",
            str(transport.get_extra_info('peername', 'ERROR'))
        )
        self.transport = transport
        self.reader = reader = asyncio.StreamReader(loop=self.loop)
        reader.set_transport(transport)
        self.writer = writer = asyncio.StreamWriter(
            transport, self, reader, self.loop
        )
        self.connected(reader, writer)

    async def _reconnect(self, node):
        if not node:
            self.target_node = None
            await self.start()

            return

        if self.retry_policy.should_retry(node):
            await self.retry_policy.wait(node)
            await self.start(target=node)
        else:
            self.log.error(
                "Reached maximum number of retry attempts on node %s", node
            )
            self.discovery.mark_failed(node)
            await self.start()

    async def _on_transport_closed(self):
        self.log.info("Connection closed gracefully, restarting")
        self.disconnected()
        await self._reconnect(self.target_node)

    async def _on_transport_error(self, exn):
        self.log.info("Connection closed with error, restarting %s", exn)
        self.disconnected()
        await self._reconnect(self.target_node)

    async def _on_connect_failed(self, exn):
        self.log.info(
            "Failed to connect to host %s with error %s restarting",
            self.target_node, exn
        )
        self.retry_policy.record_failure(self.target_node)
        await self._reconnect(self.target_node)

    async def _on_failed_heartbeat(self, exn):
        self.log.warn("Failed to handle a heartbeat")
        self.heartbeat_failures += 1

        if self.heartbeat_failures >= 3:
            await self.reconnect()
            self.heartbeat_failures = 0

    async def _on_successful_heartbeat(self, conversation_id):
        self.log.debug(
            "Received heartbeat from conversation %s", conversation_id
        )
        self.heartbeat_failures = 0

    async def _on_connector_failed(self, exn):
        self.log.error("Connector failed to find a connection")
        await self.stop(exn=exn)

    async def _run(self):
        while True:
            try:
                msg = await self.ctrl_queue.get()
                self.log.debug("Connector received message %s", msg)

                if msg.command == ConnectorCommand.Connect:
                    await self._attempt_connect(msg.data)

                if msg.command == ConnectorCommand.HandleConnectFailure:
                    await self._on_connect_failed(msg.data)

                if msg.command == ConnectorCommand.HandleConnectionOpened:
                    await self._on_transport_received(msg.data)

                if msg.command == ConnectorCommand.HandleConnectionClosed:
                    await self._on_transport_closed()

                if msg.command == ConnectorCommand.HandleConnectionFailed:
                    await self._on_transport_closed()

                if msg.command == ConnectorCommand.HandleHeartbeatFailed:
                    await self._on_failed_heartbeat(msg.data)

                if msg.command == ConnectorCommand.HandleHeartbeatSuccess:
                    await self._on_successful_heartbeat(msg.data)

                if msg.command == ConnectorCommand.HandleConnectorFailed:
                    await self._on_connector_failed(msg.data)

                if msg.command == ConnectorCommand.Stop:
                    self.stopped(msg.data)
                    msg.future.set_result(None)

                    return
            except:  # noqa: E722
                self.log.exception('Unexpected error in connector')


class StreamingIterator:

    def __init__(self, size):
        self.items = asyncio.Queue(maxsize=size)
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
        try:
            return await self.__anext__()
        except StopAsyncIteration:
            pass

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

    async def athrow(self, e):
        await self.items.put(e)

    async def asend(self, m):
        await self.items.put(m)

    def cancel(self):
        self.finished = True
        self.asend(StopIteration())


class PersistentSubscription(convo.PersistentSubscription):

    def __init__(self, subscription, iterator, conn, out_queue=None):
        super().__init__(
            subscription.name, subscription.stream,
            subscription.conversation_id, subscription.initial_commit_position,
            subscription.last_event_number, subscription.buffer_size,
            subscription.auto_ack
        )
        self.connection = conn
        self.events = iterator
        self.out_queue = out_queue

    async def ack(self, event):
        payload = proto.PersistentSubscriptionAckEvents()
        payload.subscription_id = self.name
        payload.processed_event_ids.append(event.original_event_id.bytes_le)
        message = msg.OutboundMessage(
            self.conversation_id,
            msg.TcpCommand.PersistentSubscriptionAckEvents,
            payload.SerializeToString(),
        )

        if self.out_queue:
            await self.out_queue.put(message)
        else:
            await self.connection.enqueue_message(message)


class MessageWriter:

    def __init__(self, queue, connector):
        connector.connected.append(self.on_connected)
        connector.disconnected.append(self.on_connection_lost)
        self._queue = queue
        self._is_connected = False
        self.next = None
        self._write_loop = None
        self._logger = logging.get_named_logger(MessageWriter)

    def on_connection_lost(self):
        self._logger.warn('Connection lost, stopping loop')
        self._is_connected = False
        self._write_loop.cancel()
        asyncio.ensure_future(self.stream_writer.drain())

    def on_connected(self, _, streamwriter):
        self._logger.debug('MessageWriter connected')
        self._is_connected = True
        self.stream_writer = streamwriter
        self._write_loop = asyncio.ensure_future(
            self._write_outbound_messages()
        )

    async def enqueue_message(self, message: msg.OutboundMessage):
        await self._queue.put(message)

    async def _write_outbound_messages(self):
        if self.next:
            self._logger.debug('Sending message %s', self.next)
            self._logger.trace('Message body is %r', self.next)
            self.stream_writer.write(self.next.header_bytes)
            self.stream_writer.write(self.next.payload)

        while self._is_connected:
            self.next = await self._queue.get()
            try:
                self._logger.debug('Sending message %s', self.next)
                self._logger.trace('Message body is %r', self.next)
                self.stream_writer.write(self.next.header_bytes)
                self.stream_writer.write(self.next.payload)
            except Exception as e:
                self._logger.error(
                    'Failed to send message %s', e, exc_info=True
                )
            try:
                await self.stream_writer.drain()
                self._logger.debug("Finished drain for %s", self.next)
            except Exception as e:
                self._logger.error(e)

    async def close(self):
        if self._is_connected:
            await self.stream_writer.drain()
            self.stream_writer.close()
            self._write_loop.cancel()


class MessageReader:

    MESSAGE_MIN_SIZE = SIZE_UINT_32 + HEADER_LENGTH
    HEAD_PACK = struct.Struct('<IBB')

    def __init__(self, queue, connector):
        self.queue = queue
        self.header_bytes = array.array('B', [0] * (self.MESSAGE_MIN_SIZE))
        self.header_bytes_required = (self.MESSAGE_MIN_SIZE)
        self.length = 0
        self.message_offset = 0
        self.conversation_id = None
        self.message_buffer = None
        self._logger = logging.get_named_logger(MessageReader)
        self._stream_reader = None

        connector.connected.append(self.on_connected)
        connector.disconnected.append(self.on_connection_lost)

    def close(self):
        self._reader_loop.cancel()

    def on_connection_lost(self):
        self._is_connected = False
        self._reader_loop.cancel()

    def on_connected(self, streamreader, _):
        self._logger.debug('MessageReader connected')
        self._is_connected = True
        self._stream_reader = streamreader

        self._reader_loop = asyncio.ensure_future(self._read_inbound_messages())

    async def _read_inbound_messages(self):
        '''Loop forever reading messages and invoking
           the operation that caused them'''

        while True:
            self._logger.debug("Waiting for data")
            data = await self._stream_reader.read(8192)
            self._logger.trace(
                'Received %d bytes from remote server:\n%s', len(data),
                msg.dump(data)
            )
            await self.process(data)

    async def process(self, chunk: bytes):
        if chunk is None:
            return
        chunk_offset = 0
        chunk_len = len(chunk)

        while chunk_offset < chunk_len:
            while self.header_bytes_required and chunk_offset < chunk_len:
                offset = self.MESSAGE_MIN_SIZE - self.header_bytes_required
                self.header_bytes[offset] = chunk[chunk_offset]
                chunk_offset += 1
                self.header_bytes_required -= 1

                if not self.header_bytes_required:
                    self._logger.insane(
                        'Read %d bytes for header', self.MESSAGE_MIN_SIZE
                    )
                    (self.length, self.cmd, self.flags) = self.HEAD_PACK.unpack(
                        self.header_bytes[0:6]
                    )

                    self.conversation_id = uuid.UUID(
                        bytes_le=(self.header_bytes[6:22].tobytes())
                    )
                    self._logger.insane(
                        'length=%d, command=%d flags=%d conversation_id=%s from header bytes=%a',
                        self.length, self.cmd, self.flags, self.conversation_id,
                        self.header_bytes
                    )

                self.message_offset = HEADER_LENGTH

            message_bytes_required = self.length - self.message_offset
            self._logger.insane(
                '%d bytes of message remaining before copy',
                message_bytes_required
            )

            if message_bytes_required > 0:
                if not self.message_buffer:
                    self.message_buffer = bytearray()

                end_span = min(chunk_len, message_bytes_required + chunk_offset)
                bytes_read = end_span - chunk_offset
                self.message_buffer.extend(chunk[chunk_offset:end_span])
                self._logger.insane('Message buffer is %s', self.message_buffer)
                message_bytes_required -= bytes_read
                self.message_offset += bytes_read
                chunk_offset = end_span

            self._logger.insane(
                '%d bytes of message remaining after copy',
                message_bytes_required
            )

            if not message_bytes_required:
                message = msg.InboundMessage(
                    self.conversation_id, self.cmd, self.message_buffer or b''
                )
                self._logger.trace('Received message %r', message)
                await self.queue.put(message)
                self.length = -1
                self.message_offset = 0
                self.conversation_id = None
                self.cmd = -1
                self.header_bytes_required = self.MESSAGE_MIN_SIZE
                self.message_buffer = None


class MessageDispatcher:

    def __init__(
            self,
            connector,
            input: asyncio.Queue = None,
            output: asyncio.Queue = None,
            loop=None
    ):
        self._loop = loop or asyncio.get_event_loop()
        self._dispatch_loop = None
        self.input = input
        self.output = output or asyncio.Queue()
        self.active_conversations = {}
        self._logger = logging.get_named_logger(MessageDispatcher)
        self._connected = False
        self._connector = connector

        connector.connected.append(self.start)
        connector.disconnected.append(self.stop)

    async def enqueue_conversation(
            self, convo: convo.Conversation
    ) -> asyncio.futures.Future:
        future = asyncio.futures.Future(loop=self._loop)
        message = convo.start()

        if not message.one_way:
            self.active_conversations[convo.conversation_id] = (convo, future)

        if self._connected:
            await self.output.put(message)

        return future

    def start(self, *args):
        self._dispatch_loop = asyncio.ensure_future(
            self._process_messages(), loop=self._loop
        )

    def stop(self):
        self._connected = False

        if self._dispatch_loop:
            self._dispatch_loop.cancel()

    async def _stop_conversations(self):
        for (conversation, fut) in self.active_conversations.values():
            action = conversation.stop()
            await self.handle_reply(conversation, fut, action)
        self.active_conversations.clear()

    def has_conversation(self, id):
        return id in self.active_conversations

    def remove(self, id):
        if id in self.active_conversations:
            del self.active_conversations[id]

    async def _process_messages(self):
        self._connected = True
        try:
            for (conversation, future) in self.active_conversations.values():
                self._logger.info("Restarting conversation %s", conversation)
                await self.output.put(conversation.start())

            while True:
                message = await self.input.get()

                if not message:
                    self._logger.trace("No message received")

                    continue

                self._logger.debug("Received message %s", message)

                if message.command == msg.TcpCommand.HeartbeatRequest.value:
                    await self.enqueue_conversation(
                        convo.Heartbeat(message.conversation_id)
                    )

                    continue

                conversation, result = self.active_conversations.get(
                    message.conversation_id, (None, None)
                )

                if message.command == msg.TcpCommand.NotHandled.value:
                    await self._connector.reconnect()

                    continue

                if not conversation:
                    self._logger.error(
                        "No conversation found for message %s", message
                    )

                    continue

                self._logger.debug(
                    'Received response to conversation %s: %s', conversation,
                    message
                )

                reply = conversation.respond_to(message)
                await self.handle_reply(conversation, result, reply)
        except asyncio.CancelledError:
            return

    async def handle_reply(
            self, conversation: convo.Conversation, result: asyncio.Future,
            reply: convo.ReplyAction
    ):

        self._logger.debug('Reply is %s', reply)

        if reply.action == convo.ReplyAction.CompleteScalar:
            result.set_result(reply.result)
            del self.active_conversations[conversation.conversation_id]

        elif reply.action == convo.ReplyAction.CompleteError:
            self._logger.warn(
                'Conversation %s received an error %s', conversation,
                reply.result
            )
            result.set_exception(reply.result)
            del self.active_conversations[conversation.conversation_id]

        elif reply.action == convo.ReplyAction.BeginIterator:
            self._logger.debug(
                'Creating new streaming iterator for %s', conversation
            )
            size, events = reply.result
            it = StreamingIterator(size * 2)
            result.set_result(it)
            await it.enqueue_items(events)
            self._logger.debug('Enqueued %d events', len(events))

        elif reply.action == convo.ReplyAction.YieldToIterator:
            self._logger.debug(
                'Yielding new events into iterator for %s', conversation
            )
            iterator = result.result()
            await iterator.enqueue_items(reply.result)

        elif reply.action == convo.ReplyAction.CompleteIterator:
            self._logger.debug(
                'Yielding final events into iterator for %s', conversation
            )
            iterator = result.result()
            await iterator.enqueue_items(reply.result)
            await iterator.asend(StopAsyncIteration())
            del self.active_conversations[conversation.conversation_id]

        elif reply.action == convo.ReplyAction.RaiseToIterator:
            iterator = result.result()
            error = reply.result
            self._logger.warning(
                "Raising error %s to iterator %s", error, iterator
            )
            await iterator.asend(error)
            del self.active_conversations[conversation.conversation_id]

        elif reply.action == convo.ReplyAction.BeginPersistentSubscription:
            self._logger.debug(
                'Starting new iterator for persistent subscription %s',
                conversation
            )
            sub = PersistentSubscription(
                reply.result, StreamingIterator(reply.result.buffer_size), self,
                self.output
            )
            result.set_result(sub)

        elif reply.action == convo.ReplyAction.YieldToSubscription:
            self._logger.debug(
                'Pushing new event for subscription %s', conversation
            )
            sub = await result
            await sub.events.enqueue(reply.result)

        elif reply.action == convo.ReplyAction.RaiseToSubscription:
            sub = await result
            self._logger.info(
                "Raising error %s to persistent subscription %s", reply.result,
                sub
            )
            await sub.events.enqueue(reply.result)

        elif reply.action == convo.ReplyAction.FinishSubscription:
            sub = await result
            self._logger.info("Completing persistent subscription %s", sub)
            await sub.events.enqueue(StopIteration())

        if reply.next_message is not None:
            await self.output.put(reply.next_message)


class Client:
    '''Top level object for interacting with Eventstore.

    The connection is the entry point to working with Photon Pump.
    It exposes high level methods that wrap the
    :class:`~photonpump.messages.Operation` types from photonpump.messages.
    '''

    def __init__(self, connector, reader, writer, dispatcher, credential=None):
        self.connector = connector
        self.dispatcher = dispatcher
        self.reader = reader
        self.writer = writer

        self.connector.connected.append(self.on_connected)
        self.connector.disconnected.append(self.on_disconnected)

        self.credential = credential
        self.outstanding_heartbeat = None
        self.heartbeat_loop = None

    async def connect(self):
        await self.connector.start()

    def on_connected(self, *args):
        self.heartbeat_loop = asyncio.ensure_future(self.send_heartbeats())

    def on_disconnected(self, *args):
        if self.heartbeat_loop:
            self.heartbeat_loop.cancel()

    async def close(self):
        fut = await self.connector.stop()
        await fut
        self.dispatcher.stop()
        await self.writer.close()
        self.reader.close()

    async def ping(self, conversation_id: uuid.UUID = None):
        cmd = convo.Ping(conversation_id=conversation_id or uuid.uuid4())
        result = await self.dispatcher.enqueue_conversation(cmd)

        return await result

    async def publish_event(
            self,
            stream,
            type,
            body=None,
            id=None,
            metadata=None,
            expected_version=-2,
            require_master=False
    ):
        event = msg.NewEvent(type, id or uuid.uuid4(), body, metadata)
        conversation = convo.WriteEvents(
            stream, [event],
            expected_version=expected_version,
            require_master=require_master
        )
        result = await self.dispatcher.enqueue_conversation(conversation)

        return await result

    async def publish(
            self,
            stream: str,
            events: Sequence[msg.NewEventData],
            expected_version=msg.ExpectedVersion.Any,
            require_master=False
    ):
        cmd = convo.WriteEvents(
            stream,
            events,
            expected_version=expected_version,
            require_master=require_master
        )
        result = await self.dispatcher.enqueue_conversation(cmd)

        return await result

    async def get_event(
            self,
            stream: str,
            resolve_links=True,
            require_master=False,
            correlation_id: uuid.UUID = None
    ):
        correlation_id = correlation_id
        cmd = convo.ReadEvent(stream, resolve_links, require_master)

        result = await self.dispatcher.enqueue_conversation(cmd)

        return await result

    async def get(
            self,
            stream: str,
            direction: msg.StreamDirection = msg.StreamDirection.Forward,
            from_event: int = 0,
            max_count: int = 100,
            resolve_links: bool = True,
            require_master: bool = False,
            correlation_id: uuid.UUID = None
    ):
        correlation_id = correlation_id
        cmd = convo.ReadStreamEvents(
            stream,
            from_event,
            max_count,
            resolve_links,
            require_master,
            direction=direction
        )
        result = await self.dispatcher.enqueue_conversation(cmd)

        return await result

    async def iter(
            self,
            stream: str,
            direction: msg.StreamDirection = msg.StreamDirection.Forward,
            from_event: int = None,
            batch_size: int = 100,
            resolve_links: bool = True,
            require_master: bool = False,
            correlation_id: uuid.UUID = None
    ):
        correlation_id = correlation_id
        cmd = convo.IterStreamEvents(
            stream, from_event, batch_size, resolve_links, direction=direction
        )
        result = await self.dispatcher.enqueue_conversation(cmd)
        iterator = await result
        async for event in iterator:
            yield event

    async def subscribe_volatile(self, stream: str):
        cmd = msg.CreateVolatileSubscription(stream, loop=self.loop)
        await self.dispatcher.enqueue_conversation(cmd)

        return await cmd.future

    async def create_subscription(
            self,
            name: str,
            stream: str,
            resolve_links: bool = True,
            start_from: int = -1,
            timeout_ms: int = 30000,
            record_statistics: bool = False,
            live_buffer_size: int = 500,
            read_batch_size: int = 500,
            buffer_size: int = 1000,
            max_retry_count: int = 10,
            prefer_round_robin: bool = False,
            checkpoint_after_ms: int = 2000,
            checkpoint_max_count: int = 1000,
            checkpoint_min_count: int = 10,
            subscriber_max_count: int = 10,
            credentials: msg.Credential = None,
            conversation_id: uuid.UUID = None,
            consumer_strategy: str = msg.ROUND_ROBIN
    ):
        cmd = convo.CreatePersistentSubscription(
            name,
            stream,
            resolve_links=resolve_links,
            start_from=start_from,
            timeout_ms=timeout_ms,
            record_statistics=record_statistics,
            live_buffer_size=live_buffer_size,
            read_batch_size=read_batch_size,
            buffer_size=buffer_size,
            max_retry_count=max_retry_count,
            prefer_round_robin=prefer_round_robin,
            checkpoint_after_ms=checkpoint_after_ms,
            checkpoint_max_count=checkpoint_max_count,
            checkpoint_min_count=checkpoint_min_count,
            subscriber_max_count=subscriber_max_count,
            credentials=credentials or self.credential,
            conversation_id=conversation_id,
            consumer_strategy=consumer_strategy
        )

        future = await self.dispatcher.enqueue_conversation(cmd)

        return await future

    async def connect_subscription(
            self,
            subscription: str,
            stream: str,
            conversation_id: Optional[uuid.UUID] = None
    ):
        cmd = convo.ConnectPersistentSubscription(
            subscription,
            stream,
            credentials=self.credential,
            conversation_id=conversation_id
        )
        future = await self.dispatcher.enqueue_conversation(cmd)

        return await future

    async def ack(self, subscription, message_id, correlation_id=None):
        cmd = msg.AcknowledgeMessages(
            subscription, [message_id],
            correlation_id,
            credentials=self.credential,
            loop=self.loop
        )
        await self.dispatcher.enqueue_conversation(cmd)

    async def send_heartbeats(self):
        heartbeat_id = uuid.uuid4()

        while True:
            logging.debug("Sending heartbeat %s to server", heartbeat_id)
            hb = convo.Heartbeat(
                heartbeat_id, direction=convo.Heartbeat.OUTBOUND
            )
            fut = await self.dispatcher.enqueue_conversation(hb)

            try:
                await asyncio.wait_for(fut, 10)
                logging.debug("Received heartbeat response from server")
                self.connector.heartbeat_received(hb.conversation_id)
                await asyncio.sleep(30)
            except asyncio.TimeoutError as e:
                logging.warning("Heartbeat %s timed out", hb.conversation_id)
                self.connector.heartbeat_failed(e)
                self.dispatcher.remove(hb.conversation_id)
            except asyncio.CancelledError:
                self.dispatcher.remove(hb.conversation_id)

                return
            except Exception as exn:
                logging.exception("Heartbeat %s failed", hb.conversation_id)
                self.dispatcher.remove(hb.conversation_id)
                self.connector.heartbeat_failed(exn)

    async def __aenter__(self):
        await self.connect()

        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


def connect(
        host='localhost',
        port=1113,
        discovery_host=None,
        discovery_port=2113,
        username=None,
        password=None,
        loop=None
):
    discovery = get_discoverer(host, port, discovery_host, discovery_port)
    connector = Connector(discovery)
    input_queue = asyncio.Queue(maxsize=100)
    output_queue = asyncio.Queue(maxsize=100)

    reader = MessageReader(input_queue, connector)
    writer = MessageWriter(output_queue, connector)
    dispatcher = MessageDispatcher(connector, input_queue, output_queue)

    credential = msg.Credential(
        username, password
    ) if username and password else None

    return Client(connector, reader, writer, dispatcher, credential=credential)
