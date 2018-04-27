import asyncio
import logging

import pytest

from photonpump.connection import ConnectionHandler

from .fake_server import FakeProtocol

HOST = 'localhost'
PORT = 9876


@pytest.mark.skip(reason="unstable test, needs attention")
@pytest.mark.asyncio
async def test_connect(event_loop: asyncio.AbstractEventLoop):

    try:
        server = FakeProtocol("server")
        client = FakeProtocol("client")
        data = client.expect(8)

        handler = ConnectionHandler(event_loop, logging.getLogger(), client)
        server_loop = await event_loop.create_server(
            lambda: server, '0.0.0.0', PORT
        )

        handler.run('localhost', PORT)

        await handler.connect()
        await client.connected
        server.write(b'DEADBEEF')
        await data

        handler.hangup()
        server_loop.close()

        assert client.connections_made == 1
        assert len(client.connection_errors) == 0
        assert client.data == [b'DEADBEEF']
    except Exception as e:
        print(e)
        assert False
