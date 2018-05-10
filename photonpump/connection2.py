import asyncio
import enum
import logging
from typing import Any, NamedTuple, Optional

from . import conversations as convo
from .discovery import DiscoveryRetryPolicy, NodeService
from . import messages as msg
from . import messages_pb2 as proto


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


class Connector(asyncio.Protocol):

    def __init__(self, discovery, ctrl_queue=None, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.discovery = discovery
        self.connected = Event()
        self.disconnected = Event()
        self.ctrl_queue = ctrl_queue or asyncio.Queue(loop=self.loop)
        self.log = logging.getLogger("photonpump.connection.Connector")
        self._run_loop = asyncio.ensure_future(self._run())
        self.reader = None
        self.writer = None
        self.transport = None
        self.heartbeat_failures = 0
        self.retry_policy = DiscoveryRetryPolicy(retries_per_node=10)
        self.target_node = None

        self._connection_lost = False
        self._paused = False
        self.state = ConnectorState.Begin

    def _put_msg(self, msg):
        asyncio.ensure_future(self.ctrl_queue.put(msg))

    def connection_made(self, transport):
        self._put_msg(
            ConnectorInstruction(
                ConnectorCommand.HandleConnectionOpened, None, transport
            )
        )

    def heartbeat_received(self, conversation_id):
        self._put_msg(
            ConnectorInstruction(
                ConnectorCommand.HandleHeartbeatSuccess, None, conversation_id
            )
        )

    def data_received(self, data):
        self.reader.feed_data(data)

    def eof_received(self):
        self.log.info("EOF received, tearing down connection")
        self.disconnected()

    def connection_lost(self, exn=None):
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

    async def start(self, target: Optional[NodeService]=None):
        self.state = ConnectorState.Connecting
        await self.ctrl_queue.put(
            ConnectorInstruction(ConnectorCommand.Connect, None, target)
        )

    async def stop(self):
        self.state = ConnectorState.Stopping
        await self.ctrl_queue.put(
            ConnectorInstruction(ConnectorCommand.Stop, None, None)
        )
        await self._run_loop

    async def _attempt_connect(self, node):
        if not node:
            node = self.target_node = await self.discovery.discover()
        self.log.info("Connecting to %s:%s", node.address, node.port)
        try:
            await self.loop.create_connection(
                lambda: self, node.address, node.port
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
        self.retry_policy.record_failure(node)

        if self.retry_policy.should_retry(node):
            await self.retry_policy.wait(node)
            await self.start(target=node)
        else:
            self.log.error("Reached maximum number of retry attempts on node %s", node)
            self.discovery.mark_failed(node)
            await self.start()

    async def _on_transport_closed(self):
        self.log.info("Connection closed gracefully, restarting")
        await self._reconnect(self.target_node)

    async def _on_transport_error(self, exn):
        self.log.info("Connection closed with error, restarting %s", exn)
        await self._reconnect(self.target_node)

    async def _on_connect_failed(self, exn):
        self.log.info(
            "Failed to connect to host %s with error %s restarting",
            self.target_node, exn
        )
        await self._reconnect(self.target_node)

    async def _on_failed_heartbeat(self, exn):
        self.log.warn("Failed to handle a heartbeat")
        self.heartbeat_failures += 1

        if self.heartbeat_failures >= 3:
            if self.transport:
                self.transport.close()

    async def _on_successful_heartbeat(self, conversation_id):
        self.log.debug(
            "Received heartbeat from conversation %s", conversation_id
        )
        self.heartbeat_failures = 0

    async def _run(self):
        while True:
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

            elif msg.command == ConnectorCommand.Stop:
                return


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


class MessageDispatcher:

    def __init__(
            self,
            input: asyncio.Queue = None,
            output: asyncio.Queue = None,
            loop=None
    ):
        self._loop = loop or asyncio.get_event_loop()
        self._dispatch_loop = None
        self.input = input
        self.output = output or asyncio.Queue()
        self.active_conversations = {}

    async def enqueue_conversation(
            self, convo: convo.Conversation
    ) -> asyncio.futures.Future:
        future = asyncio.futures.Future(loop=self._loop)
        message = convo.start()
        self.active_conversations[convo.conversation_id] = (convo, future)
        await self.output.put(message)

        return future

    def start(self):
        self._dispatch_loop = asyncio.ensure_future(
            self._process_messages(), loop=self._loop
        )

    def stop(self):
        if self._dispatch_loop:
            self._dispatch_loop.cancel()

    def has_conversation(self, id):
        return id in self.active_conversations

    async def _process_messages(self):
        log = logging.getLogger("photonpump.connection.MessageDispatcher")

        while True:
            message = await self.input.get()

            if not message:
                log.trace("No message received")

                continue

            log.debug("Received message %s", message)

            if message.command == msg.TcpCommand.HeartbeatRequest.value:
                await self.enqueue_conversation(
                    convo.Heartbeat(message.conversation_id)
                )

                continue

            conversation, result = self.active_conversations.get(
                message.conversation_id, (None, None)
            )

            if not conversation:
                log.error("No conversation found for message %s", message)

                continue

            log.debug(
                'Received response to conversation %s: %s', conversation,
                message
            )

            reply = conversation.respond_to(message)

            log.debug('Reply is %s', reply)

            if reply.action == convo.ReplyAction.CompleteScalar:
                result.set_result(reply.result)
                del self.active_conversations[message.conversation_id]

            elif reply.action == convo.ReplyAction.CompleteError:
                log.warn(
                    'Conversation %s received an error %s', conversation,
                    reply.result
                )
                result.set_exception(reply.result)
                del self.active_conversations[message.conversation_id]

            elif reply.action == convo.ReplyAction.BeginIterator:
                log.debug(
                    'Creating new streaming iterator for %s', conversation
                )
                size, events = reply.result
                it = StreamingIterator(size * 2)
                result.set_result(it)
                await it.enqueue_items(events)
                log.debug('Enqueued %d events', len(events))

            elif reply.action == convo.ReplyAction.YieldToIterator:
                log.debug(
                    'Yielding new events into iterator for %s', conversation
                )
                iterator = result.result()
                log.debug(iterator)
                log.debug(reply.result)
                await iterator.enqueue_items(reply.result)

            elif reply.action == convo.ReplyAction.CompleteIterator:
                log.debug(
                    'Yielding final events into iterator for %s', conversation
                )
                iterator = result.result()
                await iterator.enqueue_items(reply.result)
                await iterator.asend(StopAsyncIteration())
                del self.active_conversations[message.conversation_id]

            elif reply.action == convo.ReplyAction.RaiseToIterator:
                iterator = result.result()
                error = reply.result
                log.warning("Raising error %s to iterator %s", error, iterator)
                await iterator.asend(error)
                del self.active_conversations[message.conversation_id]

            elif reply.action == convo.ReplyAction.BeginPersistentSubscription:
                log.debug(
                    'Starting new iterator for persistent subscription %s',
                    conversation
                )
                sub = PersistentSubscription(
                    reply.result, StreamingIterator(reply.result.buffer_size),
                    self, self.output
                )
                result.set_result(sub)

            elif reply.action == convo.ReplyAction.YieldToSubscription:
                log.debug('Pushing new event for subscription %s', conversation)
                sub = await result
                await sub.events.enqueue(reply.result)

            elif reply.action == convo.ReplyAction.RaiseToSubscription:
                sub = await result
                log.info(
                    "Raising error %s to persistent subscription %s",
                    reply.result, sub
                )
                await sub.events.enqueue(reply.result)

            elif reply.action == convo.ReplyAction.FinishSubscription:
                sub = await result
                log.info("Completing persistent subscription %s", sub)
                await sub.events.enqueue(StopIteration())
