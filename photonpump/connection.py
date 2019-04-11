import array
import asyncio
import enum
import logging
import struct
import uuid
from typing import Any, NamedTuple, Optional, Sequence, Union

from . import conversations as convo
from . import messages as msg
from .discovery import DiscoveryRetryPolicy, NodeService, get_discoverer, select_random

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
        name=None,
        loop=None,
    ):
        self.name = name
        self.connection_counter = 0
        self.dispatcher = dispatcher
        self.loop = loop or asyncio.get_event_loop()
        self.discovery = discovery
        self.connected = Event()
        self.disconnected = Event()
        self.stopped = Event()
        self.ctrl_queue = ctrl_queue or asyncio.Queue(loop=self.loop)
        self.log = logging.get_named_logger(Connector)
        self._run_loop = asyncio.ensure_future(self._run())
        self.heartbeat_failures = 0
        self.connect_timeout = connect_timeout
        self.active_protocol = None
        self.retry_policy = retry_policy or DiscoveryRetryPolicy(retries_per_node=0)

    def _put_msg(self, msg):
        asyncio.ensure_future(self.ctrl_queue.put(msg))

    def connection_made(self, address, protocol):
        self._put_msg(
            ConnectorInstruction(
                ConnectorCommand.HandleConnectionOpened, None, (address, protocol)
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
        self.log.info("connection_lost {}".format(exn))
        self.retry_policy.record_failure(self.target_node)

        if exn:
            self._put_msg(
                ConnectorInstruction(ConnectorCommand.HandleConnectionFailed, None, exn)
            )
        else:
            self._put_msg(
                ConnectorInstruction(
                    ConnectorCommand.HandleConnectionClosed, None, None
                )
            )

    def heartbeat_failed(self, exn=None):
        self._put_msg(
            ConnectorInstruction(ConnectorCommand.HandleHeartbeatFailed, None, exn)
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
                node,
                self.connection_counter,
                self.dispatcher,
                self,
                self.loop,
                self.name,
            )
            await asyncio.wait_for(
                self.loop.create_connection(lambda: protocol, node.address, node.port),
                self.connect_timeout,
            )
        except Exception as e:
            await self.ctrl_queue.put(
                ConnectorInstruction(ConnectorCommand.HandleConnectFailure, None, e)
            )

    async def _on_transport_received(self, address, protocol):
        self.log.info(
            "PhotonPump is connected to eventstore instance at %s via %s",
            address,
            protocol,
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
            self.log.error("Reached maximum number of retry attempts on node %s", node)
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
            self.target_node,
            exn,
        )
        self.retry_policy.record_failure(self.target_node)
        await self._reconnect(self.target_node)

    async def _on_failed_heartbeat(self, exn):
        self.log.warning("Failed to handle a heartbeat")
        self.heartbeat_failures += 1

        if self.heartbeat_failures >= 3:
            await self.active_protocol.stop()
            self.heartbeat_failures = 0

    async def _on_successful_heartbeat(self, conversation_id):
        self.log.debug("Received heartbeat from conversation %s", conversation_id)
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


class PaceMaker:
    """
    Handles heartbeat requests and responses to keep the connection alive.
    """

    def __init__(
        self,
        output_queue: asyncio.Queue,
        connector: Connector,
        response_timeout=10,
        heartbeat_period=30,
        heartbeat_id=None,
    ) -> None:
        self._output = output_queue
        self.heartbeat_id = heartbeat_id or uuid.uuid4()
        self._connector = connector
        self.response_timeout = response_timeout
        self.heartbeat_period = heartbeat_period
        self._fut = None

    async def handle_request(self, message: msg.InboundMessage):
        response = convo.Heartbeat(message.conversation_id)
        await response.start(self._output)

        return

    async def handle_response(self, message: msg.InboundMessage):
        if message.conversation_id == self.heartbeat_id:
            if self._fut:
                self._fut.set_result(message.conversation_id)

    async def send_heartbeat(self) -> asyncio.Future:
        fut = asyncio.Future()
        logging.debug("Sending heartbeat %s to server", self.heartbeat_id)
        hb = convo.Heartbeat(self.heartbeat_id, direction=convo.Heartbeat.OUTBOUND)
        await hb.start(self._output)
        self._fut = fut

        return fut

    async def await_heartbeat_response(self):
        try:
            await asyncio.wait_for(self._fut, self.response_timeout)
            logging.debug("Received heartbeat response from server")
            self._connector.heartbeat_received(self.heartbeat_id)
        except asyncio.TimeoutError as e:
            logging.warning("Heartbeat %s timed out", self.heartbeat_id)
            self._connector.heartbeat_failed(e)
        except asyncio.CancelledError:
            logging.debug("Heartbeat waiter cancelled.")
            raise
        except Exception as exn:
            logging.exception("Heartbeat %s failed", self.heartbeat_id)
            self._connector.heartbeat_failed(exn)

    async def send_heartbeats(self):
        while True:
            try:
                await self.send_heartbeat()
                await self.await_heartbeat_response()
                await asyncio.sleep(self.heartbeat_period)
            except asyncio.CancelledError:
                logging.debug("Heartbeat loop cancelled")

                break


class MessageWriter:
    def __init__(
        self,
        writer: asyncio.StreamWriter,
        connection_number: int,
        output_queue: asyncio.Queue,
        name=None,
        loop=None,
    ):
        self._logger = logging.get_named_logger(MessageWriter, name, connection_number)
        self.writer = writer
        self._queue = output_queue

    async def enqueue_message(self, message: msg.OutboundMessage):
        await self._queue.put(message)

    async def start(self):

        while True:
            msg = await self._queue.get()
            try:
                self._logger.debug("Sending message %s", msg)
                self._logger.trace("Message body is %r", msg)
                self.writer.write(msg.header_bytes)
                self.writer.write(msg.payload)
            except Exception as e:
                self._logger.error("Failed to send message %s", e, exc_info=True)
            try:
                await self.writer.drain()
                self._logger.debug("Finished drain for %s", msg)
            except Exception as e:
                self._logger.error(e)


class MessageReader:

    MESSAGE_MIN_SIZE = SIZE_UINT_32 + HEADER_LENGTH
    HEAD_PACK = struct.Struct("<IBB")

    def __init__(
        self,
        reader: asyncio.StreamReader,
        connection_number: int,
        queue,
        pacemaker: PaceMaker,
        name=None,
        loop=None,
    ):
        self._loop = loop or asyncio.get_event_loop()
        self.header_bytes = array.array("B", [0] * (self.MESSAGE_MIN_SIZE))
        self.header_bytes_required = self.MESSAGE_MIN_SIZE
        self.queue = queue
        self.length = 0
        self.message_offset = 0
        self.conversation_id = None
        self.message_buffer = None
        self._logger = logging.get_named_logger(MessageReader, name, connection_number)
        self.reader = reader
        self.pacemaker = pacemaker
        self._trace_enabled = self._logger.getEffectiveLevel() <= logging.TRACE

    def feed_data(self, data):
        self.reader.feed_data(data)

    async def start(self):
        """Loop forever reading messages and invoking
           the operation that caused them"""

        while True:
            try:
                data = await self.reader.read(8192)

                if self._trace_enabled:
                    self._logger.trace(
                        "Received %d bytes from remote server:\n%s",
                        len(data),
                        msg.dump(data),
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
                        "Read %d bytes for header", self.MESSAGE_MIN_SIZE
                    )
                    (self.length, cmd_val, self.flags) = self.HEAD_PACK.unpack(
                        self.header_bytes[0:6]
                    )
                    self.cmd = msg.TcpCommand(cmd_val)

                    self.conversation_id = uuid.UUID(
                        bytes_le=(self.header_bytes[6:22].tobytes())
                    )
                    self._logger.insane(
                        "length=%d, command=%d flags=%d conversation_id=%s from header bytes=%a",
                        self.length,
                        self.cmd,
                        self.flags,
                        self.conversation_id,
                        self.header_bytes,
                    )

                self.message_offset = HEADER_LENGTH

            message_bytes_required = self.length - self.message_offset
            self._logger.insane(
                "%d of message remaining before copy", message_bytes_required
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
                "%d bytes of message remaining after copy", message_bytes_required
            )

            if not message_bytes_required:
                message = msg.InboundMessage(
                    self.conversation_id, self.cmd, self.message_buffer or b""
                )
                self._logger.trace("Received message %r", message)

                if message.command == msg.TcpCommand.HeartbeatRequest:
                    await self.pacemaker.handle_request(message)
                elif message.command == msg.TcpCommand.HeartbeatResponse:
                    await self.pacemaker.handle_response(message)
                else:
                    await self.queue.put(message)

                self.length = -1
                self.message_offset = 0
                self.conversation_id = None
                self.cmd = -1
                self.header_bytes_required = self.MESSAGE_MIN_SIZE
                self.message_buffer = None


class MessageDispatcher:
    def __init__(self, name=None, loop=None):
        self.active_conversations = {}
        self._logger = logging.get_named_logger(MessageDispatcher, name)
        self.output = None
        self._loop = loop or asyncio.get_event_loop()

    async def start_conversation(
        self, conversation: convo.Conversation
    ) -> asyncio.futures.Future:

        if not conversation.one_way:
            self.active_conversations[conversation.conversation_id] = (
                conversation,
                None,
            )

        if self.output:
            await conversation.start(self.output)

        return conversation.result

    async def write_to(self, output: asyncio.Queue):
        self._logger.info(
            "Dispatcher has new message writer. Re-sending %s conversations",
            len(self.active_conversations),
        )
        self.output = output

        for (conversation, _) in self.active_conversations.values():
            await conversation.start(self.output)

    async def dispatch(self, message: msg.InboundMessage, output: asyncio.Queue):
        self._logger.debug("Received message %s", message)

        conversation, result = self.active_conversations.get(
            message.conversation_id, (None, None)
        )

        if not conversation:
            self._logger.error("No conversation found for message %s", message)

            return

        await conversation.respond_to(message, output)

        if conversation.is_complete:
            del self.active_conversations[conversation.conversation_id]

    def has_conversation(self, id):
        return id in self.active_conversations

    def remove(self, id):
        if id in self.active_conversations:
            del self.active_conversations[id]


class Client:
    """Top level object for interacting with Eventstore.

    The client is the entry point to working with Photon Pump.
    It exposes high level methods that wrap the
    :class:`~photonpump.conversations.Conversation` types from
    photonpump.conversations.
    """

    def __init__(self, connector, dispatcher, credential=None):
        self.connector = connector
        self.dispatcher = dispatcher

        self.credential = credential
        self.outstanding_heartbeat = None

    async def connect(self):
        """
        Sends a start message to the Connector.
        """
        await self.connector.start()

    async def close(self):
        """
        Stop the client from sending and receiving messages.
        """
        await self.connector.stop()

    async def ping(self, conversation_id: uuid.UUID = None) -> float:
        """
        Send a message to the remote server to check liveness.

        Returns:
            The round-trip time to receive a Pong message in fractional seconds

        Examples:

            >>> async with connect() as conn:
            >>>     print("Sending a PING to the server")
            >>>     time_secs = await conn.ping()
            >>>     print("Received a PONG after {} secs".format(time_secs))

        """
        cmd = convo.Ping(conversation_id=conversation_id or uuid.uuid4())
        result = await self.dispatcher.start_conversation(cmd)

        return await result

    async def publish_event(
        self,
        stream: str,
        type: str,
        body: Optional[Any] = None,
        id: Optional[uuid.UUID] = None,
        metadata: Optional[Any] = None,
        expected_version: int = -2,
        require_master: bool = False,
    ) -> None:
        """
        Publish a single event to the EventStore.

        This method publishes a single event to the remote server and waits

        for acknowledgement.

        Args:
            stream: The stream to publish the event to.
            type: the event's type.
            body: a serializable body for the event.
            id: a unique id for the event. PhotonPump will automatically generate an
                id if none is provided.
            metadata: Optional serializable metadata block for the event.
            expected_version: Used for concurrency control.
                If a positive integer is provided, EventStore will check that the stream
                is at that version before accepting a write.

                There are three magic values:
                    -4: StreamMustExist. Checks that the stream already exists.
                    -2: Any. Disables concurrency checks
                    -1: NoStream. Checks that the stream does not yet exist.
                    0: EmptyStream. Checks that the stream has been explicitly created but
                    does not yet contain any events.
            require_master: If true, slave nodes will reject this message.

        Examples:

            >>> async with connect() as conn:
            >>>     await conn.publish_event(
            >>>         "inventory_item-1",
            >>>         "item_created",
            >>>         body={ "item-id": 1, "created-date": "2018-08-19" },
            >>>         expected_version=ExpectedVersion.StreamMustNotExist
            >>>     )
            >>>
            >>>     await conn.publish_event(
            >>>         "inventory_item-1",
            >>>         "item_deleted",
            >>>         expected_version=1,
            >>>         metadata={'deleted-by': 'bob' }
            >>>     )
        """
        event = msg.NewEvent(type, id or uuid.uuid4(), body, metadata)
        conversation = convo.WriteEvents(
            stream,
            [event],
            expected_version=expected_version,
            require_master=require_master,
        )
        result = await self.dispatcher.start_conversation(conversation)

        return await result

    async def publish(
        self,
        stream: str,
        events: Sequence[msg.NewEventData],
        expected_version=msg.ExpectedVersion.Any,
        require_master=False,
    ):
        cmd = convo.WriteEvents(
            stream,
            events,
            expected_version=expected_version,
            require_master=require_master,
        )
        result = await self.dispatcher.start_conversation(cmd)

        return await result

    async def get_event(
        self,
        stream: str,
        event_number: int,
        resolve_links=True,
        require_master=False,
        correlation_id: uuid.UUID = None,
    ) -> msg.Event:
        """
        Get a single event by stream and event number.

        Args:
            stream: The name of the stream containing the event.
            event_number: The sequence number of the event to read.
            resolve_links (optional): True if eventstore should
                automatically resolve Link Events, otherwise False.
            required_master (optional): True if this command must be
                sent direct to the master node, otherwise False.
            correlation_id (optional): A unique identifer for this
                command.

        Returns:
            The resolved event if found, else None.

        Examples:

            >>> async with connection() as conn:
            >>>     await conn.publish("inventory_item-1", "item_created")
            >>>     event = await conn.get_event("inventory_item-1", 1)
            >>>     print(event)
        """
        correlation_id = correlation_id or uuid.uuid4()
        cmd = convo.ReadEvent(
            stream,
            event_number,
            resolve_links,
            require_master,
            conversation_id=correlation_id,
        )

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
        correlation_id: uuid.UUID = None,
    ):
        """
        Read a range of events from a stream.

        Args:
            stream: The name of the stream to read
            direction (optional): Controls whether to read events forward or backward.
              defaults to Forward.
            from_event (optional): The first event to read.
              defaults to the beginning of the stream when direction is forward
              and the end of the stream if direction is backward.
            max_count (optional): The maximum number of events to return.
            resolve_links (optional): True if eventstore should
                automatically resolve Link Events, otherwise False.
            required_master (optional): True if this command must be
                sent direct to the master node, otherwise False.
            correlation_id (optional): A unique identifer for this command.

        Examples:

            Read 5 events from a stream

            >>> async for event in conn.get("my-stream", max_count=5):
            >>>     print(event)


            Read events 21 to 30

            >>> async for event in conn.get("my-stream", max_count=10, from_event=21):
            >>>     print(event)

            Read 10 most recent events in reverse order

            >>> async for event in conn.get(
                        "my-stream",
                        max_count=10,
                        direction=StreamDirection.Backward
                    ):
            >>>     print(event)
        """
        correlation_id = correlation_id
        cmd = convo.ReadStreamEvents(
            stream,
            from_event,
            max_count,
            resolve_links,
            require_master,
            direction=direction,
        )
        result = await self.dispatcher.start_conversation(cmd)

        return await result

    async def get_all(
        self,
        direction: msg.StreamDirection = msg.StreamDirection.Forward,
        from_position: Optional[Union[msg.Position, msg._PositionSentinel]] = None,
        max_count: int = 100,
        resolve_links: bool = True,
        require_master: bool = False,
        correlation_id: uuid.UUID = None,
    ):
        """
        Read a range of events from the whole database.

        Args:
            direction (optional): Controls whether to read events forward or backward.
              defaults to Forward.
            from_position (optional): The position to read from.
              defaults to the beginning of the stream when direction is forward
              and the end of the stream if direction is backward.
            max_count (optional): The maximum number of events to return.
            resolve_links (optional): True if eventstore should
                automatically resolve Link Events, otherwise False.
            required_master (optional): True if this command must be
                sent direct to the master node, otherwise False.
            correlation_id (optional): A unique identifer for this command.

        Examples:

            Read 5 events

            >>> async for event in conn.get_all(max_count=5):
            >>>     print(event)


            Read 10 most recent events in reverse order

            >>> async for event in conn.get_all(
                        max_count=10,
                        direction=StreamDirection.Backward
                    ):
            >>>     print(event)
        """
        correlation_id = correlation_id
        cmd = convo.ReadAllEvents(
            msg.Position.for_direction(direction, from_position),
            max_count,
            resolve_links,
            require_master,
            direction=direction,
            credentials=self.credential,
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
        correlation_id: uuid.UUID = None,
    ):
        """
        Read through a stream of events until the end and then stop.

        Args:
            stream: The name of the stream to read.
            direction: Controls whether to read forward or backward through the
              stream. Defaults to  StreamDirection.Forward
            from_event: The sequence number of the first event to read from the
              stream. Reads from the appropriate end of the stream if unset.
            batch_size: The maximum number of events to read at a time.
            resolve_links (optional): True if eventstore should
                automatically resolve Link Events, otherwise False.
            required_master (optional): True if this command must be
                sent direct to the master node, otherwise False.
            correlation_id (optional): A unique identifer for this
                command.

        Examples:

            Print every event from the stream "my-stream".

            >>> with async.connect() as conn:
            >>>     async for event in conn.iter("my-stream"):
            >>>         print(event)

            Print every event from the stream "my-stream" in reverse order

            >>> with async.connect() as conn:
            >>>     async for event in conn.iter("my-stream", direction=StreamDirection.Backward):
            >>>         print(event)

            Skip the first 10 events of the stream

            >>> with async.connect() as conn:
            >>>     async for event in conn.iter("my-stream", from_event=11):
            >>>         print(event)
        """

        correlation_id = correlation_id or uuid.uuid4()
        cmd = convo.IterStreamEvents(
            stream,
            from_event,
            batch_size,
            resolve_links,
            direction=direction,
            credentials=self.credential,
        )
        result = await self.dispatcher.start_conversation(cmd)
        iterator = await result
        async for event in iterator:
            yield event

    async def iter_all(
        self,
        direction: msg.StreamDirection = msg.StreamDirection.Forward,
        from_position: Optional[Union[msg.Position, msg._PositionSentinel]] = None,
        batch_size: int = 100,
        resolve_links: bool = True,
        require_master: bool = False,
        correlation_id: Optional[uuid.UUID] = None,
    ):
        """
        Read through all the events in the database.

        Args:
            direction (optional): Controls whether to read forward or backward
              through the events. Defaults to  StreamDirection.Forward
            from_position (optional): The position to start reading from.
              Defaults to photonpump.Beginning when direction is Forward,
              photonpump.End when direction is Backward.
            batch_size (optional): The maximum number of events to read at a time.
            resolve_links (optional): True if eventstore should
                automatically resolve Link Events, otherwise False.
            required_master (optional): True if this command must be
                sent direct to the master node, otherwise False.
            correlation_id (optional): A unique identifer for this
                command.

        Examples:

            Print every event from the database.

            >>> with async.connect() as conn:
            >>>     async for event in conn.iter_all()
            >>>         print(event)

            Print every event from the database in reverse order

            >>> with async.connect() as conn:
            >>>     async for event in conn.iter_all(direction=StreamDirection.Backward):
            >>>         print(event)

            Start reading from a known commit position

            >>> with async.connect() as conn:
            >>>     async for event in conn.iter_all(from_position=Position(12345))
            >>>         print(event)

        """
        correlation_id = correlation_id

        cmd = convo.IterAllEvents(
            msg.Position.for_direction(direction, from_position),
            batch_size,
            resolve_links,
            require_master,
            direction,
            self.credential,
            correlation_id,
        )
        result = await self.dispatcher.start_conversation(cmd)
        iterator = await result
        async for event in iterator:
            yield event

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
        consumer_strategy: str = msg.ROUND_ROBIN,
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
            consumer_strategy=consumer_strategy,
        )

        future = await self.dispatcher.start_conversation(cmd)

        return await future

    async def connect_subscription(
        self,
        subscription: str,
        stream: str,
        conversation_id: Optional[uuid.UUID] = None,
    ):
        cmd = convo.ConnectPersistentSubscription(
            subscription,
            stream,
            credentials=self.credential,
            conversation_id=conversation_id,
        )
        future = await self.dispatcher.start_conversation(cmd)

        return await future

    async def subscribe_to(
        self, stream, start_from=-1, resolve_link_tos=True, batch_size: int = 100
    ):
        """
        Subscribe to receive notifications when a new event is published
        to a stream.

        Args:
            stream: The name of the stream.
            start_from (optional): The first event to read.
                This parameter defaults to the magic value -1 which is treated
                as meaning "from the end of the stream". IF this value is used,
                no historical events will be returned.

                For any other value, photonpump will read all events from
                start_from until the end of the stream in pages of max_size
                before subscribing to receive new events as they arrive.

            resolve_links (optional): True if eventstore should
                automatically resolve Link Events, otherwise False.
            required_master (optional): True if this command must be
                sent direct to the master node, otherwise False.
            correlation_id (optional): A unique identifer for this
                command.
            batch_size (optioal): The number of events to pull down from
                eventstore in one go.

        Returns:
            A VolatileSubscription.

        Examples:

            >>> async with connection() as conn:
            >>>     # Subscribe only to NEW events on the cpu-metrics stream
            >>>     subs = await conn.subscribe_to("price-changes")
            >>>     async for event in subs.events:
            >>>         print(event)

            >>> async with connection() as conn:
            >>>     # Read all historical events and then receive updates as they
            >>>     # arrive.
            >>>     subs = await conn.subscribe_to("price-changes", start_from=0)
            >>>     async for event in subs.events:
            >>>         print(event)
        """

        if start_from == -1:
            cmd: convo.Conversation = convo.SubscribeToStream(
                stream, resolve_link_tos, credentials=self.credential
            )
        else:
            cmd = convo.CatchupSubscription(
                stream, start_from, batch_size, credential=self.credential
            )

        future = await self.dispatcher.start_conversation(cmd)

        return await future

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
        loop,
        name,
    ):
        self.name = name
        self._log = logging.get_named_logger(
            PhotonPumpProtocol, self.name, connection_number
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
        stream_writer = asyncio.StreamWriter(transport, self, stream_reader, self.loop)
        self.pacemaker = PaceMaker(self.output_queue, self.connector)

        self.reader = MessageReader(
            stream_reader,
            self.connection_number,
            self.input_queue,
            self.pacemaker,
            name=self.name,
        )
        self.writer = MessageWriter(
            stream_writer, self.connection_number, self.output_queue, name=self.name
        )

        self.write_loop = asyncio.ensure_future(self.writer.start())
        self.read_loop = asyncio.ensure_future(self.reader.start())
        self.dispatch_loop = asyncio.ensure_future(self.dispatch())
        self.heartbeat_loop = asyncio.ensure_future(self.pacemaker.send_heartbeats())
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
            self.heartbeat_loop.cancel()
            self._log.debug("Waiting for coroutines to end")
            await asyncio.gather(
                self.read_loop,
                self.write_loop,
                self.dispatch_loop,
                self.heartbeat_loop,
                loop=self.loop,
                return_exceptions=True,
            )
            self.transport.close()
            self._log.debug("Closed the transport")
        except asyncio.CancelledError:
            pass


def connect(
    host="localhost",
    port=1113,
    discovery_host=None,
    discovery_port=2113,
    username=None,
    password=None,
    loop=None,
    name=None,
    selector=select_random,
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

            The discovery host returns gossip data about the cluster. We use the
            gossip to select a node at random from the avaialble cluster members.

            If you're using
            :meth:`persistent subscriptions <photonpump.connection.Client.create_subscription>`
            you will always want to connect to the master node of the cluster.
            The selector parameter is a function that chooses an available node from
            the gossip result. To select the master node, use the
            :func:`photonpump.discovery.prefer_master` function. This function will return
            the master node if there is a live master, and a random replica otherwise.
            All requests to the server can be made with the require_master flag which
            will raise an error if the current node is not a master.

            >>> async with connect(
            >>>     discovery_host="eventstore.test",
            >>>     selector=discovery.prefer_master,
            >>> ) as c:
            >>>     await c.ping(require_master=True)

            Conversely, you might want to avoid connecting to the master node for reasons
            of scalability. For this you can use the
            :func:`photonpump.discovery.prefer_replica` function.

            >>> async with connect(
            >>>     discovery_host="eventstore.test",
            >>>     selector=discovery.prefer_replica,
            >>> ) as c:
            >>>     await c.ping()


            For some operations, you may need to authenticate your requests by
            providing a username and password to the client.

            >>> async with connect(username='admin', password='changeit') as c:
            >>>     await c.ping()

            Ordinarily you will create a single Client per application, but for
            advanced scenarios you might want multiple connections. In this
            situation, you can name each connection in order to get better logging.

            >>> async with connect(name="event-reader"):
            >>>     await c.ping()

            >>> async with connect(name="event-writer"):
            >>>     await c.ping()


        Args:
            host: The IP or DNS entry to connect with, defaults to 'localhost'.
            port: The port to connect with, defaults to 1113.
            discovery_host: The IP or DNS entry to use for cluster discovery.
            discovery_port: The port to use for cluster discovery, defaults to 2113.
            username: The username to use when communicating with eventstore.
            password: The password to use when communicating with eventstore.
            loop:An Asyncio event loop.
            selector: An optional function that selects one element from a list of
                :class:`photonpump.disovery.DiscoveredNode` elements.

    """
    discovery = get_discoverer(host, port, discovery_host, discovery_port, selector)
    dispatcher = MessageDispatcher(name=name, loop=loop)
    connector = Connector(discovery, dispatcher, name=name)

    credential = msg.Credential(username, password) if username and password else None

    return Client(connector, dispatcher, credential=credential)
