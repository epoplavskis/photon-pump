"""
The Pacemaker exists to send and receive heartbeats.

The MessageDispatcher used to receive heartbeats and reply to them but
this leads to dropped connections if we're not processing our received
messages quickly enough.

While that's a primitive form of flow control, it's sort of annoying that
an application-level issue can cause connection-level outages, so we
introduce the pacemaker.

The MessageReader receives HeartbeatRequest commands from the server and
gives them to the Pacemaker, who sends a reply to the MessageWriter.

Periodically, the Pacemaker sends his own HeartbeatRequest to the server
and waits for a response, raising a TimeoutError after a configurable period.

This allows us to detect half-closed TCP connections.
"""


import asyncio
import pytest
import uuid
from photonpump.connection import PaceMaker
from photonpump.messages import InboundMessage, OutboundMessage, TcpCommand

from .test_connector import TeeQueue
from ..fakes import FakeConnector


@pytest.mark.asyncio
async def test_when_receiving_a_heartbeat_request():
    """
    When we receive a HeartbeatRequest from the server, we should
    immediately respond with a HeartbeatResponse
    """
    out_queue = TeeQueue()
    pace_maker = PaceMaker(out_queue, None)

    heartbeat_id = uuid.uuid4()

    await pace_maker.handle_request(
        InboundMessage(heartbeat_id, TcpCommand.HeartbeatRequest, bytes())
    )

    response = await out_queue.get()
    assert response == OutboundMessage(
        heartbeat_id, TcpCommand.HeartbeatResponse, bytes()
    )


@pytest.mark.asyncio
async def test_when_sending_a_heartbeat_request():
    """
    Periodically, the Pacemaker sends a heartbeat request to the server.
    """

    heartbeat_id = uuid.uuid4()
    out_queue = TeeQueue()
    pace_maker = PaceMaker(out_queue, None, heartbeat_id=heartbeat_id)

    await pace_maker.send_heartbeat()

    [request] = out_queue.items

    assert request == OutboundMessage(
        heartbeat_id, TcpCommand.HeartbeatRequest, bytes()
    )


@pytest.mark.asyncio
async def test_when_the_heartbeat_times_out():
    """
    If the heartbeat times, out we should tell the connector.
    """

    heartbeat_id = uuid.uuid4()
    out_queue = TeeQueue()
    connector = FakeConnector()
    pace_maker = PaceMaker(out_queue, connector, heartbeat_id=heartbeat_id)

    timeout = asyncio.TimeoutError("lol, timed out, sorry")

    fut = await pace_maker.send_heartbeat()
    fut.set_exception(timeout)

    await pace_maker.await_heartbeat_response()

    assert connector.failures == [timeout]
    assert connector.successes == 0


@pytest.mark.asyncio
async def test_when_the_heartbeat_fails():
    """
    If the heartbeat fails because lol random, we should tell the connector.
    """

    heartbeat_id = uuid.uuid4()
    out_queue = TeeQueue()
    connector = FakeConnector()
    pace_maker = PaceMaker(out_queue, connector, heartbeat_id=heartbeat_id)

    exn = KeyError("How even could this happen?")

    fut = await pace_maker.send_heartbeat()
    fut.set_exception(exn)

    await pace_maker.await_heartbeat_response()

    assert connector.failures == [exn]
    assert connector.successes == 0


@pytest.mark.asyncio
async def test_when_the_heartbeat_is_cancelled():
    """
    If the heartbeat fails with a cancellation error we shouldn't
    worry about it.
    """

    heartbeat_id = uuid.uuid4()
    out_queue = TeeQueue()
    connector = FakeConnector()
    pace_maker = PaceMaker(out_queue, connector, heartbeat_id=heartbeat_id)

    fut = await pace_maker.send_heartbeat()
    fut.cancel()

    with pytest.raises(asyncio.CancelledError):
        await pace_maker.await_heartbeat_response()

    assert connector.failures == []
    assert connector.successes == 0


@pytest.mark.asyncio
async def test_when_the_heartbeat_succeeds():
    """
    If the heartbeat receives a response within the timeout period
    we should record the success.
    """

    heartbeat_id = uuid.uuid4()
    out_queue = TeeQueue()
    connector = FakeConnector()
    pace_maker = PaceMaker(out_queue, connector, heartbeat_id=heartbeat_id)

    await pace_maker.send_heartbeat()
    await pace_maker.handle_response(
        InboundMessage(heartbeat_id, TcpCommand.HeartbeatResponse, bytes())
    )

    await pace_maker.await_heartbeat_response()

    assert connector.failures == []
    assert connector.successes == 1
