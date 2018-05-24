"""
This module tests the Connector. He's a cool guy. He tries to connect to
Eventstore and doesn't quit until he's exhausted every option. He only deals
with TCP connections, StreamReaders, and StreamWriters, so we can test him with
a simple echo server.

When he makes a connection, loses a connection, or quits trying, he raises an
event. These events are handled by the Reader, Writer, and Dispatcher.
"""

import asyncio

import logging
import pytest

from photonpump.connection import Connector, ConnectorCommand
from photonpump.conversations import Ping
from photonpump.discovery import (
    DiscoveryFailed, NodeService, SingleNodeDiscovery
)

from ..fakes import EchoServer, TeeQueue, SpyDispatcher


async def connector_event(connector_event):
    fut = asyncio.Future()

    def _cb(*args):
        if not fut.done():
            fut.set_result(None)

    connector_event.append(_cb)

    return await fut


@pytest.mark.asyncio
async def test_when_connecting_to_a_server(event_loop):
    """
    When we connect to a server, the protocol should begin sending the
    pending messages held by the dispatcher.

    When it receives an InboundMessage from the MessageReader, it should call
    the "dispatch" method of the Dispatcher.
    """

    addr = NodeService("localhost", 8338, None)

    async with EchoServer(addr, event_loop):

        dispatcher = SpyDispatcher()
        connector = Connector(
            SingleNodeDiscovery(addr), dispatcher, loop=event_loop
        )

        ping = Ping()

        result = await dispatcher.start_conversation(ping)

        await connector.start()
        await connector_event(connector.connected)
        await result

        await connector.stop()


@pytest.mark.asyncio
async def test_when_a_server_disconnects(event_loop):
    """
    Usually, when eventstore goes away, we'll get an EOF on the transport.
    If that happens, we should raise a disconnected event.

    We should also place a reconnection message on the control queue.
    """

    addr = NodeService("localhost", 8338, None)
    queue = TeeQueue()

    dispatcher = SpyDispatcher()
    connector = Connector(
        SingleNodeDiscovery(addr),
        dispatcher,
        loop=event_loop,
        ctrl_queue=queue
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
    dispatcher = SpyDispatcher()
    connector = Connector(
        SingleNodeDiscovery(addr),
        dispatcher,
        loop=event_loop,
        ctrl_queue=queue
    )

    async with EchoServer(addr, event_loop):
        await connector.start()
        [connect, connected] = await queue.next_event(count=2)
        assert connect.command == ConnectorCommand.Connect
        assert connected.command == ConnectorCommand.HandleConnectionOpened

        connector.heartbeat_failed()
        connector.heartbeat_failed()
        connector.heartbeat_failed()

        [hb1, hb2, hb3, connection_closed,
         reconnect] = await queue.next_event(count=5)

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
    dispatcher = SpyDispatcher()
    connector = Connector(
        SingleNodeDiscovery(addr),
        dispatcher,
        loop=event_loop,
        ctrl_queue=queue
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
    dispatcher = SpyDispatcher()
    connector = Connector(
        SingleNodeDiscovery(addr),
        dispatcher,
        loop=event_loop,
        ctrl_queue=queue,
        retry_policy=policy
    )

    connector.stopped.append(on_stopped)

    await connector.start()
    [connect, connection_failed] = await queue.next_event(count=2)

    [reconnect, failed] = await asyncio.wait_for(queue.next_event(count=2), 2)
    assert failed.command == ConnectorCommand.HandleConnectorFailed
    assert policy.recorded == addr
    assert isinstance(await wait_for_stopped, DiscoveryFailed)


@pytest.mark.asyncio
async def test_when_the_connection_fails_with_an_error(event_loop):
    queue = TeeQueue()
    addr = NodeService("localhost", 8338, None)
    dispatcher = SpyDispatcher()
    connector = Connector(
        SingleNodeDiscovery(addr),
        dispatcher,
        loop=event_loop,
        ctrl_queue=queue
    )

    async with EchoServer(addr, event_loop):

        await connector.start()
        [connect, connected] = await queue.next_event(count=2)

        exn = ValueError()
        connector.connection_lost(exn)

        [connection_failed] = await queue.next_event(count=1)

        assert connection_failed.command == ConnectorCommand.HandleConnectionFailed
        assert connection_failed.data is exn


@pytest.mark.asyncio
async def test_when_restarting_a_running_connector(event_loop):
    queue = TeeQueue()
    addr = NodeService("localhost", 8338, None)
    dispatcher = SpyDispatcher()
    connector = Connector(
        SingleNodeDiscovery(addr),
        dispatcher,
        loop=event_loop,
        ctrl_queue=queue
    )

    async with EchoServer(addr, event_loop):
        await connector.start()
        [connect, connected] = await queue.next_event(count=2)

        assert connected.command == ConnectorCommand.HandleConnectionOpened
        await connector_event(connector.connected)

        await connector.reconnect()

        [closed, reconnect] = await queue.next_event(count=2)

        assert closed.command == ConnectorCommand.HandleConnectionClosed
        assert reconnect.command == ConnectorCommand.Connect


@pytest.mark.asyncio
async def test_when_restarting_a_stopped_connector(event_loop):
    queue = TeeQueue()
    addr = NodeService("localhost", 8338, None)
    dispatcher = SpyDispatcher()
    connector = Connector(
        SingleNodeDiscovery(addr),
        dispatcher,
        loop=event_loop,
        ctrl_queue=queue
    )

    async with EchoServer(addr, event_loop):
        await connector.reconnect()
        [connect, connected] = await queue.next_event(count=2)

        assert connect.command == ConnectorCommand.Connect
        assert connected.command == ConnectorCommand.HandleConnectionOpened
        await connector_event(connector.connected)

        await connector.stop()
