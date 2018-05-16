"""
This module tests the Connector. He's a cool guy. He tries to connect to
Eventstore and doesn't quit until he's exhausted every option. He only deals
with TCP connections, StreamReaders, and StreamWriters, so we can test him with
a simple echo server.

When he makes a connection, loses a connection, or quits trying, he raises an
event. These events are handled by the Reader, Writer, and Dispatcher.
"""

import asyncio

import pytest

from photonpump.connection import Connector, ConnectorCommand
from photonpump.discovery import NodeService, SingleNodeDiscovery, DiscoveryFailed


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

    def __init__(self, cb):
        self.cb = cb

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))
        self.transport = transport
        self.cb(transport)

    def data_received(self, data):
        # message = data
        print('Data received: {!r}'.format(data))

        # print('Send: {!r}'.format(message))
        self.transport.write(data)

        # print('Close the client socket')
        # self.transport.close()


class EchoServer:

    def __init__(self, addr, loop):
        self.host = addr.address
        self.port = addr.port
        self.loop = loop
        self.running = False

    async def __aenter__(self):
        self.transports = []
        server = self.loop.create_server(
            self.make_protocol, self.host, self.port
        )
        self._server = await server
        self.running = True

        return self

    def make_protocol(self):
        return EchoServerClientProtocol(self.transports.append)

    async def __aexit__(self, exc_type, exc, tb):
        self.stop()

    def stop(self):
        if not self.running:
            return

        for transport in self.transports:
            transport.close()
        self._server.close()
        self.running = False


@pytest.mark.asyncio
async def test_when_connecting_to_a_server(event_loop):
    """
    When we connect to a server, we should receive an event
    with a reader/writer pair as the callback args.
    """

    addr = NodeService("localhost", 8338, None)

    async with EchoServer(addr, event_loop):

        connector = Connector(SingleNodeDiscovery(addr), loop=event_loop)
        events = []
        wait_for = asyncio.Future(loop=event_loop)

        def on_connected(reader, writer):
            print("Called!")
            events.append((reader, writer))
            wait_for.set_result(None)

        connector.connected.append(on_connected)
        await connector.start()

        await asyncio.wait_for(wait_for, 2)
        assert len(events) == 1
        print(events[0])

        reader, writer = events[0]
        writer.write("Hello".encode())

        received = await asyncio.wait_for(reader.read(100), 1)
        assert received.decode() == "Hello"

        await connector.stop()


@pytest.mark.asyncio
async def test_when_a_server_disconnects(event_loop):
    """
    Usually, when eventstore goes away, we'll get an EOF on the transport.
    If that happens, we should raise a disconnected event.

    We should also place a reconnection message on the queue.
    """

    addr = NodeService("localhost", 8338, None)
    queue = TeeQueue()

    connector = Connector(
        SingleNodeDiscovery(addr), loop=event_loop, ctrl_queue=queue
    )
    raised_disconnected_event = asyncio.Future(loop=event_loop)

    def on_disconnected():
        raised_disconnected_event.set_result(True)

    connector.disconnected.append(on_disconnected)

    async with EchoServer(addr, event_loop) as server:

        await connector.start()
        connect = await queue.next_event()
        assert connect.command == ConnectorCommand.Connect

        connected = await queue.next_event()
        assert connected.command == ConnectorCommand.HandleConnectionOpened

        server.stop()

        disconnect = await queue.next_event()
        assert disconnect.command == ConnectorCommand.HandleConnectionClosed

        reconnect = await queue.next_event()
        assert reconnect.command == ConnectorCommand.Connect

    assert raised_disconnected_event.result() is True
    await connector.stop()


@pytest.mark.asyncio
async def test_when_three_heartbeats_fail_in_a_row(event_loop):
    """
    We're going to set up a separate heartbeat loop to send heartbeat requests
    to the server. If three of those heartbeats timeout in a row, we'll put a
    reconnection request on the queue.
    """

    queue = TeeQueue()
    addr = NodeService("localhost", 8338, None)
    connector = Connector(
        SingleNodeDiscovery(addr), loop=event_loop, ctrl_queue=queue
    )

    async with EchoServer(addr, event_loop):
        await connector.start()
        [connect, connected] = await queue.next_event(count=2)
        assert connect.command == ConnectorCommand.Connect
        assert connected.command == ConnectorCommand.HandleConnectionOpened

        connector.heartbeat_failed()
        connector.heartbeat_failed()
        connector.heartbeat_failed()

        [hb1, hb2, hb3, connection_closed, reconnect] = await queue.next_event(count=5)

        assert connection_closed.command == ConnectorCommand.HandleConnectionClosed
        assert reconnect.command == ConnectorCommand.Connect

    await connector.stop()


@pytest.mark.asyncio
async def test_when_a_heartbeat_succeeds(event_loop):
    """
    If one of our heartbeats succeeds, we should reset our counter.
    Ergo, if we have two failures, followed by a success, followed
    by two failures, we should not reset the connection.
    """

    queue = TeeQueue()
    addr = NodeService("localhost", 8338, None)
    connector = Connector(
        SingleNodeDiscovery(addr), loop=event_loop, ctrl_queue=queue
    )

    async with EchoServer(addr, event_loop):
        await connector.start()
        [connect, connected] = await queue.next_event(count=2)
        assert connect.command == ConnectorCommand.Connect
        assert connected.command == ConnectorCommand.HandleConnectionOpened

        connector.heartbeat_failed()
        connector.heartbeat_failed()

        [hb1, hb2] = await queue.next_event(count=2)
        assert connector.heartbeat_failures == 2

        connector.heartbeat_received("Foo")

        connector.heartbeat_failed()
        connector.heartbeat_failed()

        [success, hb3, hb4] = await queue.next_event(count=3)

        assert connector.heartbeat_failures == 2
        assert success.command == ConnectorCommand.HandleHeartbeatSuccess
        assert hb3.command == ConnectorCommand.HandleHeartbeatFailed
        assert hb4.command == ConnectorCommand.HandleHeartbeatFailed

        await connector.stop()


@pytest.mark.asyncio
async def test_when_discovery_fails_on_reconnection(event_loop):

    """
    If we can't retry our current node any more, and we can't discover a new one
    then it's game over and we should raise the stopped event.
    """

    class never_retry:

        def should_retry(self, _):
            return False

        def record_failure(self, node):
            self.recorded = node

    wait_for_stopped = asyncio.Future()

    def on_stopped(exn):
        wait_for_stopped.set_result(exn)

    queue = TeeQueue()
    addr = NodeService("localhost", 8338, None)
    policy = never_retry()
    connector = Connector(
        SingleNodeDiscovery(addr), loop=event_loop, ctrl_queue=queue,
        retry_policy=policy
    )

    connector.stopped.append(on_stopped)

    await connector.start()
    [connect, connection_failed] = await queue.next_event(count=2)

    [reconnect, failed, stop] = await asyncio.wait_for(queue.next_event(count=3), 2)
    assert failed.command == ConnectorCommand.HandleConnectorFailed
    assert policy.recorded == addr
    assert isinstance(await wait_for_stopped, DiscoveryFailed)
