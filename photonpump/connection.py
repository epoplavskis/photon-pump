import array
import asyncio
import logging
import random
import struct
import uuid
from typing import Sequence

from . import conversations as convo
from . import messages as msg

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


MESSAGE_MIN_SIZE = SIZE_UINT_32 + HEADER_LENGTH


async def process(queue):
    while True:
        header_bytes = array.array('B', [0] * (MESSAGE_MIN_SIZE))
        header_bytes_required = (MESSAGE_MIN_SIZE)
        length = 0
        message_offset = 0
        conversation_id = None
        message = None

        chunk = yield
        if not chunk:
            continue
        chunk_offset = 0
        chunk_len = len(chunk)

        print("new chunk")
        print(
            "chunk_offset=%d chunk_len=%d length=%d" %
            (chunk_offset, chunk_len, length)
        )

        while chunk_offset < chunk_len:
            while header_bytes_required and chunk_offset < chunk_len:
                header_bytes[MESSAGE_MIN_SIZE
                             - header_bytes_required] = chunk[chunk_offset]
                chunk_offset += 1
                header_bytes_required -= 1

                if not header_bytes_required:
                    (length, cmd,
                     flags) = struct.unpack('<IBB', header_bytes[0:6])

                    conversation_id = uuid.UUID(
                        bytes_le=(header_bytes[6:22].tobytes())
                    )

                    print("new header")
                    print(
                        "chunk_offset=%d chunk_len=%d length=%d" %
                        (chunk_offset, chunk_len, length)
                    )
                message_offset = HEADER_LENGTH

            message_bytes_required = length - message_offset
            print(message_bytes_required)

            if message_bytes_required > 0:
                print("needs moar bytes!")
                print(
                    "chunk_offset=%d chunk_len=%d length=%d message_bytes_required=%d"
                    % (chunk_offset, chunk_len, length, message_bytes_required)
                )

                if not message:
                    message = bytearray(message_bytes_required)

                end_span = min(chunk_len, message_bytes_required + chunk_offset)
                bytes_read = end_span - chunk_offset
                message.extend(chunk[chunk_offset:end_span])
                message_bytes_required -= bytes_read
                message_offset += bytes_read
                chunk_offset = end_span
                print("needs moar bytes?")
                print(
                    "chunk_offset=%d chunk_len=%d length=%d message_bytes_required=%d"
                    % (chunk_offset, chunk_len, length, message_bytes_required)
                )

            if not message_bytes_required:
                print("raising message")
                await queue.put(msg.InboundMessage(conversation_id, cmd, bytearray()))
                length = -1
                message_offset = 0
                conversation_id = None
                cmd = -1
                header_bytes_required = MESSAGE_MIN_SIZE
                message = None
                print(
                    "chunk_offset=%d chunk_len=%d length=%d message_bytes_required=%d"
                    % (chunk_offset, chunk_len, length, message_bytes_required)
                )


class ConnectionHandler:

    CONNECT = 1
    DISCONNECT = 2
    RECONNECT = 3
    OK = 4
    HANGUP = 5

    def __init__(self, loop, logger, protocol, max_attempts=100):
        self.queue = asyncio.Queue(maxsize=1)
        self._loop = loop
        self._logger = logger
        self._protocol = protocol
        self._last_message = None
        self._current_attempts = 0
        self._max_attempts = max_attempts
        self._run_loop = None

    async def connect(self):
        await self.queue.put(ConnectionHandler.CONNECT)

    def hangup(self):
        self._logger.debug("Connection hanging up")
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
        while (True):
            msg = await self.queue.get()

            if (msg == ConnectionHandler.OK):
                self._logger.debug("Eventstore connection is OK")
                self._current_attempts = 0
            elif (msg == ConnectionHandler.CONNECT):
                await self._attempt_connect(host, port)
            elif msg == ConnectionHandler.HANGUP:
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
            await self._loop.create_connection(self.getProtocol, host, port)
        except Exception as e:
            self._logger.error(e)

            if self._current_attempts == self._max_attempts:
                raise
            await self._attempt_connect(host, port)

    def getProtocol(self):
        return self._protocol


class EventstoreProtocol(asyncio.streams.FlowControlMixin):

    def __init__(self, host, port, queue, pending, logger=None, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._host = host
        self._is_connecting = False
        self._port = port
        self._logger = logger or logging.getLogger(__name__)
        self._queue = queue
        self._pending_operations = pending
        self._pending_responses = asyncio.Queue(maxsize=128, loop=self._loop)
        self._read_loop = None
        self._write_loop = None
        self._parse_loop = process(self._pending_responses)
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
            self._read_responses(), loop=self._loop
        )
        self._write_loop = asyncio.ensure_future(
            self._process_queue(), loop=self._loop
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

    async def enqueue(self, conversation: convo.Conversation):
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

    async def _process_queue(self):
        if self.next:
            self.next.send(self._writer)

        while self._is_connected:
            self.next = await self._queue.get()
            try:
                self._logger.debug(msg.dump(self.next.header_bytes, self.next.payload))
                self._writer.write(self.next.header_bytes)
                self._writer.write(self.next.payload)
                self._logger.debug("Sent outbound message %s", self.next)
            except Exception as e:
                self._logger.error(
                    "Failed to send message %s", e, exc_info=True
                )
            try:
                await self._writer.drain()
            except Exception as e:
                self._logger.error(e)

    async def _read_responses(self):
        """Loop forever reading messages and invoking
           the operation that caused them"""

        try:
            await self._parse_loop.asend(None)
        except Exception as e:
            self._logger.error(e)

        while True:
            print("Fur")
            data = await self._reader.read(8192)

            print("For")
            msg.dump(data)
            print("Far")
            try:
                await self._parse_loop.asend(data)
            except Exception as e:
                self._logger.error(e, exc_info=True)
            print("Fir")

    async def _process_responses(self):
        while True:
            message = await self._pending_responses.get()
            self._logger.info("I got a message! %s", message)
            if not message:
                continue
            if message.command == msg.TcpCommand.HeartbeatRequest:
                self._logger.info("Can has heartbeat plx?")
                await self._queue.put(msg.OutboundMessage(message.conversation_id,
                    msg.TcpCommand.HeartbeatResponse,
                    b''))
                continue

            conversation, result = self._pending_operations.get(message.conversation_id, (None,None))

            if conversation is None:
                self._logger.error(
                    "No conversations can handle message %s", message
                )

                continue
            self._logger.debug("Received response to conversation %s", conversation)

            reply = conversation.respond_to(message)
            self._logger.debug("Reply is %s", reply)

            if reply.action == convo.ReplyAction.CompleteScalar:
                result.set_result(reply.result)
                del self._pending_operations[message.conversation_id]
            elif reply.action == convo.ReplyAction.CompleteError:
                result.set_exception(reply.result)
                del self._pending_operations[message.conversation_id]

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

    async def ping(self, correlation_id: uuid.UUID = None):
        correlation_id = correlation_id
        cmd = msg.Ping(correlation_id=correlation_id, loop=self.loop)
        await self.protocol.enqueue(cmd)

        return await cmd.future

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
        cmd = msg.WriteEvents(
            stream, [event],
            expected_version=expected_version,
            require_master=require_master,
            loop=self.loop
        )
        await self.protocol.enqueue(cmd)

        return await cmd.future

    async def publish(
            self,
            stream: str,
            events: Sequence[msg.NewEventData],
            expected_version=msg.ExpectedVersion.Any,
            require_master=False
    ):
        cmd = msg.WriteEvents(
            stream,
            events,
            expected_version=expected_version,
            require_master=require_master,
            loop=self.loop
        )
        await self.protocol.enqueue(cmd)

        return await cmd.future

    async def get_event(
            self,
            stream: str,
            resolve_links=True,
            require_master=False,
            correlation_id: uuid.UUID = None
    ):
        correlation_id = correlation_id
        cmd = msg.ReadEvent(
            stream, resolve_links, require_master, loop=self.loop
        )
        await self.protocol.enqueue(cmd)

        return await cmd.future

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
        cmd = msg.ReadStreamEvents(
            stream,
            from_event,
            max_count,
            resolve_links,
            require_master,
            direction=direction,
            loop=self.loop
        )
        await self.protocol.enqueue(cmd)

        return await cmd.future

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
        cmd = msg.IterStreamEvents(
            stream,
            from_event,
            batch_size,
            resolve_links,
            require_master,
            loop=self.loop
        )
        await self.protocol.enqueue(cmd)
        async for e in cmd.iterator:
            yield e

    async def subscribe_volatile(self, stream: str):
        cmd = msg.CreateVolatileSubscription(stream, loop=self.loop)
        await self.protocol.enqueue(cmd)

        return await cmd.future

    async def create_subscription(self, stream: str, name: str):
        cmd = convo.CreatePersistentSubscription(
            stream, name, credentials=self.credential
        )

        return await self.protocol.enqueue(cmd)

    async def connect_subscription(self, subscription: str, stream: str):
        cmd = convo.ConnectPersistentSubscription(
            subscription, stream, credentials=self.credential
        )
        future = await self.protocol.enqueue(cmd)

        return await future

    async def ack(self, subscription, message_id, correlation_id=None):
        cmd = msg.AcknowledgeMessages(
            subscription, [message_id],
            correlation_id,
            credentials=self.credential,
            loop=self.loop
        )
        await self.protocol.enqueue(cmd)


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
