import asyncio
import enum
import logging

from . import conversations as convo
from . import messages as msg
from . import messages_pb2 as proto


class Event(list):

    def __call__(self, *args, **kwargs):
        for f in self:
            f(*args, **kwargs)

    def __repr__(self):
        return 'Event(%s)' % list.__repr__(self)


class Connector(asyncio.Protocol):

    class cmd(enum.IntEnum):
        Connect = 0
        Stop = -1

    def __init__(self, discovery, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.discovery = discovery
        self.connected = Event()
        self.disconnected = Event()
        self.ctrl_queue = asyncio.Queue(loop=self.loop)
        self.log = logging.getLogger("photonpump.connection.Connector")
        self._run_loop = asyncio.ensure_future(self._run())
        self.reader = None
        self.writer = None

        self._connection_lost = False
        self._paused = False

    def connection_made(self, transport):
        self.log.info(
            "PhotonPump is connected to eventstore instance at %s",
            str(transport.get_extra_info('peername', 'ERROR'))
        )
        self.reader = reader = asyncio.StreamReader(loop=self.loop)
        reader.set_transport(transport)
        self.writer = writer = asyncio.StreamWriter(transport, self, reader, self.loop)
        self.connected(reader, writer)

    def data_received(self, data):
        self.reader.feed_data(data)

    async def start(self):
        await self.ctrl_queue.put(Connector.cmd.Connect)

    async def stop(self):
        await self.ctrl_queue.put(Connector.cmd.Stop)
        await self._run_loop

    async def _run(self):
        while True:
            self.log.info("Waiting for a message")
            msg = await self.ctrl_queue.get()

            if msg == Connector.cmd.Connect:
                self.log.info("Connecting")
                node = await self.discovery.discover()
                self.log.info("Connecting to %s:%s", node.address, node.port)
                await self.loop.create_connection(
                        lambda: self, node.address, node.port
                )
            elif msg == Connector.cmd.Stop:
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
