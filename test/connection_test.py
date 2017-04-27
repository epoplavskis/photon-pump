import asyncio
import pytest
from photonpump import Connection

@pytest.yield_fixture
def loop():

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()

def test_connect(loop):

    connected = False
    closed = False

    def _on_connected():
       nonlocal connected
       connected = True

    def _on_closed():
        nonlocal closed
        closed = True

    async def do_test():
       conn = Connection(loop=loop)
       conn.connected.append(_on_connected)
       conn.disconnected.append(_on_closed)

       await conn.connect()
       print("a")
       assert connected

       conn.close()
       assert closed

    loop.run_until_complete(do_test())


def test_ping(loop):

    async def do_test(loop):
       conn = Connection(loop=loop)
       await conn.connect()
       pong = await conn.ping()

       assert pong
       conn.close()

    loop.run_until_complete(do_test(loop))
