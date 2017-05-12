import uuid

import pytest

from photonpump import Connection, connect


@pytest.mark.asyncio
async def test_connection_events(event_loop):

    connected = False
    closed = False

    def _on_connected():
        nonlocal connected
        connected = True

    def _on_closed():
        nonlocal closed
        closed = True

    conn = Connection(loop=event_loop)
    conn.connected.append(_on_connected)
    conn.disconnected.append(_on_closed)

    await conn.connect()
    assert connected

    conn.close()
    assert closed


@pytest.mark.asyncio
async def test_ping(event_loop):

    conn = Connection(loop=event_loop)
    await conn.connect()

    pong = await conn.ping()
    assert pong

    conn.close()


@pytest.mark.asyncio
async def test_ping_context_mgr(event_loop):

    async with connect(loop=event_loop) as conn:
        id = uuid.uuid4()
        pong = await conn.ping(correlation_id=id)
        assert pong.correlation_id == id
