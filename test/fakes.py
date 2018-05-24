import asyncio
import logging

from photonpump.connection import Event
from photonpump.conversations import MagicConversation


class TeeQueue:

    def __init__(self):
        self.items = []
        self.queue = asyncio.Queue()
        self.teed_queue = asyncio.Queue()

    async def get(self):
        return await self.queue.get()

    async def put(self, item):
        self.items.append(item)
        await self.queue.put(item)
        await self.teed_queue.put(item)

    async def next_event(self, count=None):
        if not count:
            return await self.teed_queue.get()
        needed = count
        result = []

        while needed > 0:
            result.append(await self.teed_queue.get())
            needed -= 1

        return result


class EchoServerClientProtocol(asyncio.Protocol):

    def __init__(self, cb, number):
        self.cb = cb
        self.number = number

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        logging.info(
            'Connection from {} to protocol {}'.format(peername, self.number)
        )
        self.transport = transport
        self.cb(transport)

    def data_received(self, data):
        # message = data
        logging.info(
            'ServerProtocol {} received data: {!r}'.format(self.number, data)
        )

        # print('Send: {!r}'.format(message))
        self.transport.write(data)

        # print('Close the client socket')
        # self.transport.close()


class EchoServer:

    def __init__(self, addr, loop):
        self.host = addr.address
        self.port = addr.port
        self.loop = loop
        self.protocol_counter = 0
        self.running = False

    async def __aenter__(self):
        self.transports = []
        server = self.loop.create_server(
            self.make_protocol, self.host, self.port
        )
        self._server = await server
        self.running = True
        logging.info("Echo server is running %s", self._server)

        return self

    def make_protocol(self):
        self.protocol_counter += 1

        return EchoServerClientProtocol(
            self.transports.append, self.protocol_counter
        )

    async def __aexit__(self, exc_type, exc, tb):
        self.stop()

    def stop(self):
        if not self.running:
            return

        for transport in self.transports:
            transport.close()
        self._server.close()
        self.running = False


class SpyDispatcher:

    def __init__(self):
        self.received = TeeQueue()
        self.pending_messages = None
        self.active_conversations = {}

    async def dispatch(self, msg, _):
        logging.info("Received inbound message %s", msg)
        await self.received.put(msg)

    async def start_conversation(self, conversation):
        if self.pending_messages:
            await self.pending_messages.put(conversation.start())
        self.active_conversations[conversation.conversation_id] = (conversation, None)

    async def write_to(self, output):
        self.pending_messages = output

        for (conversation, _) in self.active_conversations.values():
            if isinstance(conversation, convo.MagicConversation):
                await conversation.start(output)
            else:
                await self.pending_messages.put(conversation.start())


class FakeConnector():

    def __init__(self, ):
        self.connected = Event()
        self.disconnected = Event()
        self.stopped = Event()
