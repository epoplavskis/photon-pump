import pytest
from uuid import uuid4

from ..fakes import TeeQueue
from photonpump.conversations import Heartbeat, Ping, ReplyAction
from photonpump.messages import HEADER_LENGTH, InboundMessage, TcpCommand


def test_start_heartbeat_conversation():

    id = uuid4()
    conversation = Heartbeat(id, direction=Heartbeat.OUTBOUND)

    request = conversation.start()

    assert not request.one_way

    assert request.length == HEADER_LENGTH
    assert request.payload == b''
    assert request.command == TcpCommand.HeartbeatRequest


def test_respond_to_server_heartbeat():

    id = uuid4()
    conversation = Heartbeat(id)

    response = conversation.start()
    assert response.one_way

    assert response.length == HEADER_LENGTH
    assert response.payload == b''
    assert response.command == TcpCommand.HeartbeatResponse

    assert conversation.direction == Heartbeat.INBOUND


def test_when_server_responds_to_heartbeat():
    id = uuid4()
    conversation = Heartbeat(id, direction=Heartbeat.OUTBOUND)

    reply = conversation.respond_to(
        InboundMessage(
            conversation.conversation_id, TcpCommand.HeartbeatResponse, b''
        )
    )

    assert reply.action == ReplyAction.CompleteScalar
    assert reply.result is True
    assert reply.next_message is None


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
