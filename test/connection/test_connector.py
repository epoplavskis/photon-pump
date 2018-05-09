"""
This module tests the Connector. He's a cool guy. He tries to connect to
Eventstore and doesn't quit until he's exhausted every option. He only deals
with TCP connections, StreamReaders, and StreamWriters, so we can test him with
a simple echo server.

When he makes a connection, loses a connection, or quits trying, he raises an
event. These events are handled by the Reader, Writer, and Dispatcher.
"""

import asyncio
import socket
import threading

import pytest

from photonpump.connection2 import Connector
from photonpump.discovery import NodeService, SingleNodeDiscovery


class EchoServerClientProtocol(asyncio.Protocol):

    def __init__(self, cb):
        self.cb = cb

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))
        self.transport = transport
        self.cb(transport)

    def data_received(self, data):
        message = data.decode()
        print('Data received: {!r}'.format(message))

        print('Send: {!r}'.format(message))
        self.transport.write(data)

        print('Close the client socket')
        self.transport.close()


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

    addr = NodeService("localhost", 8338, None)

    connector = Connector(SingleNodeDiscovery(addr), loop=event_loop)
    wait_for = asyncio.Future(loop=event_loop)

    def on_disconnected():
        print("disconnected")
        wait_for.set_result(None)

    connector.disconnected.append(on_disconnected)
 
    async with EchoServer(addr, event_loop) as server:

        def on_connected(*args):
            print("connected")
            server.stop()

        connector.connected.append(on_connected)

        await connector.start()
        await asyncio.wait_for(wait_for, 2)
        await connector.stop()
