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
import threading

from photonpump.connection2 import Connector
from photonpump.discovery import NodeService, SingleNodeDiscovery


class EchoServer:

    def __init__(self, addr, loop):
        self.server = None
        self.loop = loop
        self.host = addr.address
        self.port = addr.port

    async def start(self):
        _server = asyncio.start_server(self._run, self.host, self.port, loop=self.loop)
        self.server = await _server
        print("Started server? %s", self.server)

    async def _run(self, reader, writer):
        print("Running?")
        data = await reader.read(100)
        print("Got mad data")
        message = data.decode()
        addr = writer.get_extra_info('peername')
        print("Received %r from %r" % (message, addr))

        print("Send: %r" % message)
        writer.write(data)
        await writer.drain()
        writer.close()

    def stop(self):
        self.server.close()


@pytest.mark.asyncio
async def test_when_connecting_to_a_server(event_loop):

    addr = NodeService("localhost", 8338, None)

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
