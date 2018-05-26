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


class Connector:

    def __init__(
            self,
            discovery,
            dispatcher,
            retry_policy=None,
            ctrl_queue=None,
            connect_timeout=5,
            loop=None
    ):
        self.connection_counter = 0
        self.dispatcher = dispatcher
        self.loop = loop or asyncio.get_event_loop()
        self.discovery = discovery
        self.connected = Event()
        self.disconnected = Event()
        self.stopped = Event()
        self.ctrl_queue = ctrl_queue or asyncio.Queue(loop=self.loop)
        self.log = logging.getLogger("photonpump.connection.Connector")
        self._run_loop = asyncio.ensure_future(self._run())
        self.heartbeat_failures = 0
        self.connect_timeout = connect_timeout
        self.active_protocol = None
        self.retry_policy = retry_policy or DiscoveryRetryPolicy(
            retries_per_node=0
        )

    def _put_msg(self, msg):
        asyncio.ensure_future(self.ctrl_queue.put(msg))

    def connection_made(self, address, protocol):
        self._put_msg(
            ConnectorInstruction(
                ConnectorCommand.HandleConnectionOpened, None,
                (address, protocol)
            )
        )

    def heartbeat_received(self, conversation_id):
        self.retry_policy.record_success(self.target_node)
        self._put_msg(
            ConnectorInstruction(
                ConnectorCommand.HandleHeartbeatSuccess, None, conversation_id
            )
        )

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

    async def start(self, target: Optional[NodeService] = None):
        self.state = ConnectorState.Connecting
        await self.ctrl_queue.put(
            ConnectorInstruction(ConnectorCommand.Connect, None, target)
        )

    async def stop(self, exn=None):
        self.log.info("Stopping connector")
        self.state = ConnectorState.Stopping
        self.log.info("In ur stop stopping ur procool")

        if self.active_protocol:
            await self.active_protocol.stop()
        self.active_protocol = None
        self._run_loop.cancel()
        self.stopped(exn)

    async def reconnect(self):
        if self.active_protocol:
            await self.active_protocol.stop()
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
            self.connection_counter += 1
            protocol = PhotonPumpProtocol(
                node, self.connection_counter, self.dispatcher, self, self.loop
            )
            await asyncio.wait_for(
                self.loop.create_connection(
                    lambda: protocol, node.address, node.port
                ), self.connect_timeout
            )
        except Exception as e:
            await self.ctrl_queue.put(
                ConnectorInstruction(
                    ConnectorCommand.HandleConnectFailure, None, e
                )
            )

    async def _on_transport_received(self, address, protocol):
        self.log.info(
            "PhotonPump is connected to eventstore instance at %s via %s",
            address, protocol
        )
        self.active_protocol = protocol
        await self.dispatcher.write_to(protocol.output_queue)
        self.connected(address)

    async def _reconnect(self, node):
        if not node:
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
            await self.active_protocol.stop()
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
            self.log.debug("Waiting for next message")
            msg = await self.ctrl_queue.get()
            self.log.debug("Connector received message %s", msg)

            if msg.command == ConnectorCommand.Connect:
                await self._attempt_connect(msg.data)

            if msg.command == ConnectorCommand.HandleConnectFailure:
                await self._on_connect_failed(msg.data)

            if msg.command == ConnectorCommand.HandleConnectionOpened:
                await self._on_transport_received(*msg.data)

            if msg.command == ConnectorCommand.HandleConnectionClosed:
                await self._on_transport_closed()

            if msg.command == ConnectorCommand.HandleConnectionFailed:
                await self._on_transport_error(msg.data)

            if msg.command == ConnectorCommand.HandleHeartbeatFailed:
                await self._on_failed_heartbeat(msg.data)

            if msg.command == ConnectorCommand.HandleHeartbeatSuccess:
                await self._on_successful_heartbeat(msg.data)

            if msg.command == ConnectorCommand.HandleConnectorFailed:
                await self._on_connector_failed(msg.data)

            if msg.command == ConnectorCommand.Stop:
                raise NotImplementedError("WAT DO?")


class StreamingIterator:

    def __init__(self, size=0):
        self.items = asyncio.Queue(size)
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

    async def asend(self, m):
        await self.items.put(m)


class PersistentSubscription(convo.PersistentSubscription):

    def __init__(self, subscription, iterator, conn, out_queue):
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

        await self.out_queue.put(message)


class MessageWriter:

    def __init__(
            self,
            writer: asyncio.StreamWriter,
            connection_number: int,
            output_queue: asyncio.Queue,
            loop=None
    ):
        self._logger = logging.get_named_logger(
            MessageWriter, connection_number
        )
        self.writer = writer
        self._queue = output_queue

    async def enqueue_message(self, message: msg.OutboundMessage):
        await self._queue.put(message)

    async def start(self):

        while True:
            msg = await self._queue.get()
            try:
                self._logger.debug('Sending message %s', msg)
                self._logger.trace('Message body is %r', msg)
                self.writer.write(msg.header_bytes)
                self.writer.write(msg.payload)
            except Exception as e:
                self._logger.error(
                    'Failed to send message %s', e, exc_info=True
                )
            try:
                await self.writer.drain()
                self._logger.debug("Finished drain for %s", msg)
            except Exception as e:
                self._logger.error(e)


class MessageReader:

    MESSAGE_MIN_SIZE = SIZE_UINT_32 + HEADER_LENGTH
    HEAD_PACK = struct.Struct('<IBB')

    def __init__(
            self,
            reader: asyncio.StreamReader,
            connection_number: int,
            queue,
            loop=None
    ):
        self._loop = loop or asyncio.get_event_loop()
        self.header_bytes = array.array('B', [0] * (self.MESSAGE_MIN_SIZE))
        self.header_bytes_required = (self.MESSAGE_MIN_SIZE)
        self.queue = queue
        self.length = 0
        self.message_offset = 0
        self.conversation_id = None
        self.message_buffer = None
        self._logger = logging.get_named_logger(
            MessageReader, connection_number
        )
        self.reader = reader

    def feed_data(self, data):
        self.reader.feed_data(data)

    async def start(self):
        '''Loop forever reading messages and invoking
           the operation that caused them'''

        while True:
            try:
                data = await self.reader.read(8192)
                self._logger.trace(
                    'Received %d bytes from remote server:\n%s', len(data),
                    msg.dump(data)
                )
                await self.process(data)
            except asyncio.CancelledError:
                return
            except:
                logging.exception("Unhandled error in Message Reader")
                raise

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
                '%d of message remaining before copy', message_bytes_required
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

    def __init__(self, loop=None):
        self.active_conversations = {}
        self._logger = logging.get_named_logger(MessageDispatcher)
        self.output = None
        self._loop = loop or asyncio.get_event_loop()

    async def start_conversation(
            self, conversation: convo.Conversation
    ) -> asyncio.futures.Future:
        if isinstance(conversation, convo.MagicConversation):

            if not conversation.one_way:
                self.active_conversations[conversation.conversation_id] = (conversation, None)
            if self.output:
                await conversation.start(self.output)
            return conversation.result

        else:

            future = asyncio.futures.Future(loop=self._loop)
            message = conversation.start()

            if not message.one_way:
                self.active_conversations[conversation.conversation_id] = (conversation, future)

            if self.output:
                await self.output.put(message)

            return future

    async def write_to(self, output: asyncio.Queue):
        self._logger.info(
            "Dispatcher has new message writer. Re-sending %s conversations",
            len(self.active_conversations)
        )
        self.output = output

        for (conversation, _) in self.active_conversations.values():
            if isinstance(conversation, convo.MagicConversation):
                await conversation.start(self.output)
            else:
                await output.put(conversation.start())

    # Todo: Is the output necessary here?
    async def dispatch(
            self, message: msg.InboundMessage, output: asyncio.Queue
    ):
        self._logger.debug("Received message %s", message)

        if message.command == msg.TcpCommand.HeartbeatRequest.value:
            response = convo.Heartbeat(message.conversation_id)
            await response.start(output)

            return

        conversation, result = self.active_conversations.get(
            message.conversation_id, (None, None)
        )

        if not conversation:
            self._logger.error("No conversation found for message %s", message)

            return

        if isinstance(conversation, convo.MagicConversation):
            await conversation.respond_to(message, output)
            if conversation.is_complete:
                del self.active_conversations[conversation.conversation_id]
            return



        self._logger.debug(
            'Received response to conversation %s: %s', conversation, message
        )
        reply = conversation.respond_to(message)
        await self.handle_reply(conversation, result, reply, output)

    def has_conversation(self, id):
        return id in self.active_conversations

    def remove(self, id):
        if id in self.active_conversations:
            del self.active_conversations[id]

    async def handle_reply(
            self, conversation: convo.Conversation, result: asyncio.Future,
            reply: convo.ReplyAction, output: asyncio.Queue
    ):

        self._logger.debug('Reply is %s', reply)

        if reply.action == convo.ReplyAction.CompleteError:
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
            # Until we figure things out, we'll run subscriptions with unbounded queues
            # cos otherwise we block in irritating places.
            sub = PersistentSubscription(
                reply.result, StreamingIterator(0), self,
                output
            )
            result.set_result(sub)

        elif reply.action == convo.ReplyAction.ContinueSubscription:
            self._logger.debug("Attaching subscription to a new connection")
            sub = await result
            sub.out_queue = output

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
            await output.put(reply.next_message)


class Client:
    '''Top level object for interacting with Eventstore.

    The client is the entry point to working with Photon Pump.
    It exposes high level methods that wrap the
    :class:`~photonpump.conversations.Conversation` types from
    photonpump.conversations.
    '''

    def __init__(self, connector, dispatcher, credential=None):
        self.connector = connector
        self.dispatcher = dispatcher

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
        await self.connector.stop()

    async def ping(self, conversation_id: uuid.UUID = None):
        cmd = convo.Ping(conversation_id=conversation_id or uuid.uuid4())
        result = await self.dispatcher.start_conversation(cmd)

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
        result = await self.dispatcher.start_conversation(conversation)

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
        result = await self.dispatcher.start_conversation(cmd)

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

        result = await self.dispatcher.start_conversation(cmd)

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
        result = await self.dispatcher.start_conversation(cmd)

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
        result = await self.dispatcher.start_conversation(cmd)
        iterator = await result
        async for event in iterator:
            yield event

    async def subscribe_volatile(self, stream: str):
        cmd = msg.CreateVolatileSubscription(stream, loop=self.loop)
        await self.dispatcher.start_conversation(cmd)

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

        future = await self.dispatcher.start_conversation(cmd)

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
        future = await self.dispatcher.start_conversation(cmd)

        return await future

    async def ack(self, subscription, message_ids, correlation_id=None):
        cmd = msg.PersistentSubscriptionAckEvents(
            subscription,
            message_ids,
            correlation_id,
            credentials=self.credential,
            loop=self.loop
        )
        await self.dispatcher.start_conversation(cmd)

    async def send_heartbeats(self):
        heartbeat_id = uuid.uuid4()

        while True:
            logging.debug("Sending heartbeat %s to server", heartbeat_id)
            hb = convo.Heartbeat(
                heartbeat_id, direction=convo.Heartbeat.OUTBOUND
            )
            fut = await self.dispatcher.start_conversation(hb)

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


class PhotonPumpProtocol(asyncio.streams.FlowControlMixin):

    def __init__(
            self,
            addr: NodeService,
            connection_number: int,
            dispatcher: MessageDispatcher,
            connector,
            loop=None
    ):
        self._log = logging.get_named_logger(
            PhotonPumpProtocol, connection_number
        )
        self.transport = None
        self.loop = loop or asyncio.get_event_loop()
        super().__init__(self.loop)
        self.connection_number = connection_number
        self.node = addr
        self.dispatcher = dispatcher
        self.connector = connector

    def connection_made(self, transport):
        self._log.debug("Connection made.")
        self.input_queue = asyncio.Queue(loop=self.loop)
        self.output_queue = asyncio.Queue(loop=self.loop)
        self.transport = transport

        stream_reader = asyncio.StreamReader(loop=self.loop)
        stream_reader.set_transport(transport)
        stream_writer = asyncio.StreamWriter(
            transport, self, stream_reader, self.loop
        )

        self.reader = MessageReader(
            stream_reader, self.connection_number, self.input_queue
        )
        self.writer = MessageWriter(
            stream_writer, self.connection_number, self.output_queue
        )

        self.write_loop = asyncio.ensure_future(self.writer.start())
        self.read_loop = asyncio.ensure_future(self.reader.start())
        self.dispatch_loop = asyncio.ensure_future(self.dispatch())
        self.connector.connection_made(self.node, self)

    def data_received(self, data):
        self.reader.feed_data(data)

    async def dispatch(self):
        while True:
            try:
                next_msg = await self.input_queue.get()
                await self.dispatcher.dispatch(next_msg, self.output_queue)
            except asyncio.CancelledError:
                break
            except:
                logging.exception("Dispatch loop failed")
                break

    def connection_lost(self, exn):
        self._log.debug("Connection lost")
        super().connection_lost(exn)
        self._connection_lost = True
        self.connector.connection_lost(exn)

    async def stop(self):
        self._log.debug("Stopping")
        try:
            self.read_loop.cancel()
            self.write_loop.cancel()
            self.dispatch_loop.cancel()
            self._log.debug("Waiting for coroutines to end")
            await asyncio.gather(
                self.read_loop,
                self.write_loop,
                self.dispatch_loop,
                loop=self.loop,
                return_exceptions=True
            )
            self.transport.close()
            self._log.debug("Closed the transport")
        except asyncio.CancelledError:
            pass


def connect(
        host='localhost',
        port=1113,
        discovery_host=None,
        discovery_port=2113,
        username=None,
        password=None,
        loop=None
) -> Client:
    """ Create a new client.

        Examples:
            Since the Client is an async context manager, we can use it in a
            with block for automatic connect/disconnect semantics.

            >>> async with connect(host='127.0.0.1', port=1113) as c:
            >>>     await c.ping()

            Or we can call connect at a more convenient moment

            >>> c = connect()
            >>> await c.connect()
            >>> await c.ping()
            >>> await c.close()

            For cluster discovery cases, we can provide a discovery host and
            port. The host may be an IP or DNS entry. If you provide a DNS
            entry, discovery will choose randomly from the registered IP
            addresses for the hostname.

            >>> async with connect(discovery_host="eventstore.test") as c:
            >>>     await c.ping()

            For some operations, you may need to authenticate your requests by
            providing a username and password to the client.

            >>> async with connect(username='admin', password='changeit') as c:
            >>>     await c.ping()

        Args:
            host: The IP or DNS entry to connect with, defaults to 'localhost'.
            port: The port to connect with, defaults to 1113.
            discovery_host: The IP or DNS entry to use for cluster discovery.
            discovery_port: The port to use for cluster discovery, defaults to 2113.
            username: The username to use when communicating with eventstore.
            password: The password to use when communicating with eventstore.
            loop:An Asyncio event loop.

    """
    discovery = get_discoverer(host, port, discovery_host, discovery_port)
    dispatcher = MessageDispatcher(loop)
    connector = Connector(discovery, dispatcher)

    credential = msg.Credential(
        username, password
    ) if username and password else None

    return Client(connector, dispatcher, credential=credential)
