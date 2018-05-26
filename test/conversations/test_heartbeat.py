import pytest
from uuid import uuid4

from ..fakes import TeeQueue
from photonpump.conversations import Heartbeat, Ping, ReplyAction
from photonpump.messages import HEADER_LENGTH, InboundMessage, TcpCommand


@pytest.mark.asyncio
async def test_start_heartbeat_conversation():

    output = TeeQueue()

    id = uuid4()
    conversation = Heartbeat(id, direction=Heartbeat.OUTBOUND)

    await conversation.start(output)
    request = await output.get()

    assert not request.one_way

    assert request.length == HEADER_LENGTH
    assert request.payload == b''
    assert request.command == TcpCommand.HeartbeatRequest


@pytest.mark.asyncio
async def test_respond_to_server_heartbeat():

    output = TeeQueue()

    id = uuid4()
    conversation = Heartbeat(id)

    await conversation.start(output)
    response = await output.get()

    assert response.one_way

    assert response.length == HEADER_LENGTH
    assert response.payload == b''
    assert response.command == TcpCommand.HeartbeatResponse

    assert conversation.direction == Heartbeat.INBOUND


@pytest.mark.asyncio
async def test_when_server_responds_to_heartbeat():

    output = TeeQueue()

    id = uuid4()
    conversation = Heartbeat(id, direction=Heartbeat.OUTBOUND)

    await conversation.respond_to(
        InboundMessage(
            conversation.conversation_id, TcpCommand.HeartbeatResponse, b''
        ), output
    )
    await conversation.result

    assert conversation.is_complete


@pytest.mark.asyncio
async def test_ping_conversation():

    conversation = Ping()
    output = TeeQueue()
    await conversation.start(output)

    request = await output.get()

    assert request.length == HEADER_LENGTH
    assert request.command == TcpCommand.Ping
    assert request.payload == b''

    assert not conversation.is_complete


@pytest.mark.asyncio
async def test_ping_response():

    output = TeeQueue()
    conversation = Ping()
    await conversation.start(output)

    await conversation.respond_to(
        InboundMessage(conversation.conversation_id, TcpCommand.Pong, b''),
        output
    )

    assert conversation.is_complete
