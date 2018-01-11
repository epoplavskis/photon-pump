from uuid import uuid4

from photonpump.messages import (
    HEADER_LENGTH, HeartbeatConversation, InboundMessage, PingConversation,
    TcpCommand, ReplyAction
)


def test_start_heartbeat_conversation():

    id = uuid4()
    conversation = HeartbeatConversation(id)

    response = conversation.start()

    assert response.length == HEADER_LENGTH
    assert response.payload == b''
    assert response.command == TcpCommand.HeartbeatResponse
    assert response.is_authenticated is False


def test_ping_conversation():

    conversation = PingConversation()
    request = conversation.start()

    assert request.length == HEADER_LENGTH
    assert request.command == TcpCommand.Ping
    assert request.is_authenticated is False
    assert request.payload == b''


def test_ping_response():

    conversation = PingConversation()
    conversation.start()

    reply = conversation.respond_to(
        InboundMessage(conversation.conversation_id, TcpCommand.Pong, b'')
    )

    assert reply.action == ReplyAction.CompleteScalar
    assert reply.result is None
    assert reply.next_message is None
