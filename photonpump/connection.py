import array
import asyncio
import logging
import random
import struct
import uuid
from typing import Sequence

from . import conversations as convo
from . import messages as msg
from . import messages_pb2 as proto

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
        return "Event(%s)" % list.__repr__(self)


class MessageReader:

    MESSAGE_MIN_SIZE = SIZE_UINT_32 + HEADER_LENGTH
    HEAD_PACK = struct.Struct('<IBB')

    def __init__(self, queue):
        self.queue = queue
        self.header_bytes = array.array('B', [0] * (self.MESSAGE_MIN_SIZE))
        self.header_bytes_required = (self.MESSAGE_MIN_SIZE)
        self.length = 0
        self.message_offset = 0
        self.conversation_id = None
        self.message_buffer = None
        self._logger = logging.get_named_logger(MessageReader)

    async def process(self, chunk: bytes):
        if chunk is None:
            return
        chunk_offset = 0
        chunk_len = len(chunk)

        while chunk_offset < chunk_len:
            while self.header_bytes_required and chunk_offset < chunk_len:
                self.header_bytes[self.MESSAGE_MIN_SIZE
                                  - self.header_bytes_required
                                 ] = chunk[chunk_offset]
                chunk_offset += 1
                self.header_bytes_required -= 1

                if not self.header_bytes_required:
                    self._logger.insane(
                        "Read %d bytes for header", self.MESSAGE_MIN_SIZE
                    )
                    (self.length, self.cmd, self.flags) = self.HEAD_PACK.unpack(
                        self.header_bytes[0:6]
                    )

                    self.conversation_id = uuid.UUID(
                        bytes_le=(self.header_bytes[6:22].tobytes())
                    )
                    self._logger.insane(
                        "length=%d, command=%d flags=%d conversation_id=%s from header bytes=%a",
                        self.length, self.cmd, self.flags, self.conversation_id,
                        self.header_bytes
                    )

                self.message_offset = HEADER_LENGTH

            message_bytes_required = self.length - self.message_offset
            self._logger.insane(
                "%d bytes of message remaining before copy",
                message_bytes_required
            )

            if message_bytes_required > 0:
                if not self.message_buffer:
                    self.message_buffer = bytearray()

                end_span = min(chunk_len, message_bytes_required + chunk_offset)
                bytes_read = end_span - chunk_offset
                self.message_buffer.extend(chunk[chunk_offset:end_span])
                self._logger.insane("Message buffer is %s", self.message_buffer)
                message_bytes_required -= bytes_read
                self.message_offset += bytes_read
                chunk_offset = end_span

            self._logger.insane(
                "%d bytes of message remaining after copy",
                message_bytes_required
            )

            if not message_bytes_required:
                message = msg.InboundMessage(
                    self.conversation_id, self.cmd, self.message_buffer or b''
                )
                self._logger.trace("Received message %r", message)
                await self.queue.put(message)
                self.length = -1
                self.message_offset = 0
                self.conversation_id = None
                self.cmd = -1
                self.header_bytes_required = self.MESSAGE_MIN_SIZE
                self.message_buffer = None


class ConnectionHandler:

    CONNECT = 1
    DISCONNECT = 2
    RECONNECT = 3
    OK = 4
    HANGUP = 5

    def __init__(self, loop, logger, protocol, max_attempts=100):
        self.queue = asyncio.Queue(maxsize=1)
        self._loop = loop
        self._logger = logger or logging.get_named_logger(ConnectionHandler)
        self._protocol = protocol
        self._last_message = None
        self._current_attempts = 0
        self._max_attempts = max_attempts
        self._run_loop = None
        self.transport = None

    async def connect(self):
        await self.queue.put(ConnectionHandler.CONNECT)

    async def reconnect(self):
        await self.queue.put(ConnectionHandler.RECONNECT)

    def hangup(self):
        self._logger.debug("Connection hanging up")

        if self.transport:
            self.transport.write_eof()
        self._run_loop.cancel()

    async def ok(self):
        if self._last_message == ConnectionHandler.OK:
            return
        await self.queue.put(ConnectionHandler.OK)

    def run(self, host, port):
        self._run_loop = asyncio.ensure_future(
            self._run(host, port), loop=self._loop
        )

    async def _run(self, host, port):
        while True:
            msg = await self.queue.get()

            if msg == ConnectionHandler.OK:
                self._logger.debug("Eventstore connection is OK")
                self._current_attempts = 0
            elif msg == ConnectionHandler.CONNECT:
                await self._attempt_connect(host, port)
            elif msg == ConnectionHandler.RECONNECT:
                self.transport.close()
                await self._attempt_connect(host, port)
            elif msg == ConnectionHandler.HANGUP:
                self.transport.close()
                self._run_loop.cancel()

                return

            self._last_message = msg

    async def _attempt_connect(self, host, port):
        self._current_attempts += 1
        self._logger.info(
            "Attempting connection to %s:%d attempt %d of %d", host, port,
            self._current_attempts, self._max_attempts
        )

        if self._last_message == ConnectionHandler.CONNECT:
            sleep_time = random.uniform(0, self._current_attempts)
            self._logger.debug("Sleeping for %d secs", sleep_time)
            await asyncio.sleep(sleep_time)
        try:
            self._last_message = ConnectionHandler.CONNECT
            self.transport, _ = await self._loop.create_connection(
                lambda: self._protocol, host, port
            )
        except Exception as e:
            self._logger.warn(e, exc_info=True)

            if self._current_attempts == self._max_attempts:
                raise
            await self._attempt_connect(host, port)


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

    def __init__(self, subscription, iterator, conn):
        super().__init__(
            subscription.name, subscription.stream,
            subscription.conversation_id, subscription.initial_commit_position,
            subscription.last_event_number, subscription.buffer_size,
            subscription.auto_ack
        )
        self.connection = conn
        self.events = iterator

    async def ack(self, event):
        payload = proto.PersistentSubscriptionAckEvents()
        payload.subscription_id = self.name
        payload.processed_event_ids.append(event.original_event_id.bytes_le)
        message = msg.OutboundMessage(
            self.conversation_id,
            msg.TcpCommand.PersistentSubscriptionAckEvents,
            payload.SerializeToString(),
        )
        await self.connection.enqueue_message(message)


class EventstoreProtocol(asyncio.streams.FlowControlMixin):

    def __init__(self, host, port, queue, pending, logger=None, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._host = host
        self._is_connecting = False
        self._port = port
        self._logger = logger or logging.get_named_logger(EventstoreProtocol)
        self._queue = queue
        self._pending_operations = pending
        self._pending_responses = asyncio.Queue(maxsize=128, loop=self._loop)
        self._read_loop = None
        self._write_loop = None
        self._message_reader = MessageReader(self._pending_responses)
        self._dispatch_loop = None
        self._connection_lost = False
        self._paused = False
        self.next = None
        self._reader = None
        self._connectHandler = ConnectionHandler(self._loop, self._logger, self)
        self._connectHandler.run(self._host, self._port)

    def connection_made(self, transport):
        self._transport = transport
        self._running = True
        self._is_connected = True
        self.is_connecting = False
        self._logger.info(
            'PhotonPump is connected to eventstore instance at ' +
            str(transport.get_extra_info('peername', 'ERROR'))
        )

        self._reader = asyncio.StreamReader(loop=self._loop)
        self._reader.set_transport(transport)

        self._writer = asyncio.StreamWriter(
            self._transport, self, None, self._loop
        )

        self._read_loop = asyncio.ensure_future(
            self._read_inbound_messages(), loop=self._loop
        )
        self._write_loop = asyncio.ensure_future(
            self._write_outbound_messages(), loop=self._loop
        )
        self._dispatch_loop = asyncio.ensure_future(
            self._process_responses(), loop=self._loop
        )

    def eof_received(self):
        self._logger.log(
            logging.INFO,
            'EOF received in EventstoreProtocol, closing connection'
        )

        if self._is_connected:
            self.close()

        if not self.is_connecting:
            asyncio.ensure_future(self.connect(), loop=self._loop)

    def data_received(self, data):
        """ Process data received from Eventstore.  """
        self._reader.feed_data(data)

    def connection_lost(self, exc):
        if exc is None:
            self._reader.feed_eof()

        else:
            self._logger.error("Lost connection to Eventstore %s" % exc)
            self.close()

            if not self.is_connecting:
                asyncio.ensure_future(self.connect(), loop=self._loop)

    async def enqueue_conversation(self, conversation: convo.Conversation):
        """Enqueue an operation.

        The operation will be added to the `pending` dict, and
            pushed onto the queue for sending.

        Args:
            message: The operation to send.
        """

        message = conversation.start()
        future = None

        if not message.one_way:
            future = asyncio.Future(loop=self._loop)
            self._pending_operations[conversation.conversation_id
                                    ] = (conversation, future)
        try:
            await self._queue.put(message)
        except Exception as e:
            self._logger.error(e)

        return future

    async def connect(self):
        await self._connectHandler.connect()

    async def enqueue_message(self, message: msg.OutboundMessage):
        await self._queue.put(message)

    async def _write_outbound_messages(self):
        if self.next:
            self._writer.write(self.next.header_bytes)
            self._writer.write(self.next.payload)

        while self._is_connected:
            self._logger.debug("Sending message %s", self.next)
            self.next = await self._queue.get()
            try:
                self._writer.write(self.next.header_bytes)
                self._writer.write(self.next.payload)
            except Exception as e:
                self._logger.error(
                    "Failed to send message %s", e, exc_info=True
                )
            try:
                await self._writer.drain()
            except Exception as e:
                self._logger.error(e)

    async def _read_inbound_messages(self):
        """Loop forever reading messages and invoking
           the operation that caused them"""

        while True:
            data = await self._reader.read(8192)
            self._logger.trace(
                "Received %d bytes from remote server:\n%s", len(data),
                msg.dump(data)
            )
            await self._message_reader.process(data)

    async def _process_responses(self):
        log = logging.get_named_logger(EventstoreProtocol, "dispatcher")

        while True:
            message = await self._pending_responses.get()

            if not message:
                log.trace("No message received")

                continue

            log.debug("Received message %s", message)

            if message.command == msg.TcpCommand.HeartbeatRequest.value:
                await self.enqueue_conversation(
                    convo.Heartbeat(message.conversation_id)
                )

                continue

            conversation, result = self._pending_operations.get(
                message.conversation_id, (None, None)
            )

            if conversation is None:
                log.error("No conversations can handle message %s", message)

                continue
            log.debug(
                "Received response to conversation %s: %s", conversation,
                message
            )

            reply = conversation.respond_to(message)
            log.debug("Reply is %s", reply)

            if reply.action == convo.ReplyAction.CompleteScalar:
                result.set_result(reply.result)
                del self._pending_operations[message.conversation_id]

            elif reply.action == convo.ReplyAction.CompleteError:
                result.set_exception(reply.result)
                del self._pending_operations[message.conversation_id]

            elif reply.action == convo.ReplyAction.BeginIterator:
                log.debug(
                    "Creating new streaming iterator for %s", conversation
                )
                size, events = reply.result
                it = StreamingIterator(size * 2)
                result.set_result(it)
                await it.enqueue_items(events)
                log.debug("Enqueued %d events", len(events))

            elif reply.action == convo.ReplyAction.YieldToIterator:
                log.debug(
                    "Yielding new events into iterator for %s", conversation
                )
                iterator = result.result()
                log.debug(iterator)
                log.debug(reply.result)
                await iterator.enqueue_items(reply.result)

            elif reply.action == convo.ReplyAction.CompleteIterator:
                log.debug(
                    "Yielding final events into iterator for %s", conversation
                )
                iterator = result.result()
                log.debug(iterator)
                log.debug(reply.result)
                await iterator.enqueue_items(reply.result)
                await iterator.asend(StopAsyncIteration())
                del self._pending_operations[message.conversation_id]

            elif reply.action == convo.ReplyAction.BeginPersistentSubscription:
                log.debug(
                    "Starting new iterator for persistent subscription %s",
                    conversation
                )
                try:
                    sub = PersistentSubscription(
                        reply.result,
                        StreamingIterator(reply.result.buffer_size), self
                    )
                    log.debug("foo")
                    result.set_result(sub)
                except Exception as e:
                    log.error(e, exc_info=True)

            elif reply.action == convo.ReplyAction.YieldToSubscription:
                log.debug("Pushing new event for subscription %s", conversation)
                log.debug(result)
                sub = await result
                log.debug(sub)
                await sub.events.enqueue(reply.result)

            if reply.next_message is not None:
                await self._queue.put(reply.next_message)

    def close(self, hangup=False):
        """Close the underlying StreamWriter and cancel pending Operations."""
        self._logger.info("Closing connection")
        self.running = False

        if hangup:
            self._connectHandler.hangup()

        for _, op in self._pending_operations.items():
            pass
            #op.cancel()

        if (self._read_loop):
            self._read_loop.cancel()
            self._dispatch_loop.cancel()
            self._write_loop.cancel()
            self._transport.close()

        self._is_connected = False


class Connection:
    """Top level object for interacting with Eventstore.

    The connection is the entry point to working with Photon Pump.
    It exposes high level methods that wrap the
    :class:`~photonpump.messages.Operation` types from photonpump.messages.
    """

    def __init__(
            self,
            host='127.0.0.1',
            port=1113,
            username=None,
            password=None,
            loop=None
    ):
        self.connected = Event()
        self.disconnected = Event()
        self.host = host
        self.port = port
        self.loop = loop
        self.conversations = {}
        self.queue = asyncio.Queue(maxsize=100)
        self.protocol = EventstoreProtocol(
            host, port, self.queue, self.conversations, loop=self.loop
        )

        if username and password:
            self.credential = msg.Credential(username, password)
        else:
            self.credential = None

    async def connect(self):
        await self.protocol.connect()
        self.connected()

    def close(self):
        self.protocol.close(True)
        self.disconnected()

    async def ping(self, conversation_id: uuid.UUID = None):
        cmd = convo.Ping(conversation_id=conversation_id or uuid.uuid4())
        result = await self.protocol.enqueue_conversation(cmd)

        return await result

    async def publish_event(
            self,
            type,
            stream,
            body=None,
            id=uuid.uuid4(),
            metadata=None,
            expected_version=-2,
            require_master=False
    ):
        event = msg.NewEvent(type, id, body, metadata)
        cmd = convo.WriteEvents(
            stream, [event],
            expected_version=expected_version,
            require_master=require_master
        )
        result = await self.protocol.enqueue_conversation(cmd)

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
        result = await self.protocol.enqueue_conversation(cmd)

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

        result = await self.protocol.enqueue_conversation(cmd)

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
        result = await self.protocol.enqueue_conversation(cmd)

        return await result

    async def iter(
            self,
            stream: str,
            direction: msg.StreamDirection = msg.StreamDirection.Forward,
            from_event: int = 0,
            batch_size: int = 100,
            resolve_links: bool = True,
            require_master: bool = False,
            correlation_id: uuid.UUID = None
    ):
        correlation_id = correlation_id
        cmd = convo.IterStreamEvents(
            stream, from_event, batch_size, resolve_links
        )
        result = await self.protocol.enqueue_conversation(cmd)
        iterator = await result
        async for event in iterator:
            yield event

    async def subscribe_volatile(self, stream: str):
        cmd = msg.CreateVolatileSubscription(stream, loop=self.loop)
        await self.protocol.enqueue_conversation(cmd)

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

        return await self.protocol.enqueue_conversation(cmd)

    async def connect_subscription(self, subscription: str, stream: str):
        cmd = convo.ConnectPersistentSubscription(
            subscription, stream, credentials=self.credential
        )
        future = await self.protocol.enqueue_conversation(cmd)

        return await future

    async def ack(self, subscription, message_id, correlation_id=None):
        cmd = msg.AcknowledgeMessages(
            subscription, [message_id],
            correlation_id,
            credentials=self.credential,
            loop=self.loop
        )
        await self.protocol.enqueue_conversation(cmd)


class ConnectionContextManager:

    def __init__(
            self,
            host='127.0.0.1',
            port=1113,
            username=None,
            password=None,
            loop=None
    ):
        self.conn = Connection(
            host=host,
            port=port,
            username=username,
            password=password,
            loop=loop
        )

    async def __aenter__(self):
        await self.conn.connect()

        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        self.conn.close()


def connect(*args, **kwargs):
    return ConnectionContextManager(*args, **kwargs)
