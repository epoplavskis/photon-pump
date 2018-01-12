from uuid import uuid4

from photonpump.conversations import Heartbeat, Ping, ReplyAction
from photonpump.messages import HEADER_LENGTH, TcpCommand, InboundMessage


def test_start_heartbeat_conversation():

    id = uuid4()
    conversation = Heartbeat(id)

    response = conversation.start()

    assert response.length == HEADER_LENGTH
    assert response.payload == b''
    assert response.command == TcpCommand.HeartbeatResponse


def test_ping_conversation():

    conversation = Ping()
    request = conversation.start()

    assert request.length == HEADER_LENGTH
    assert request.command == TcpCommand.Ping
    assert request.payload == b''


def test_ping_response():

    conversation = Ping()
    conversation.start()

    reply = conversation.respond_to(
        InboundMessage(conversation.conversation_id, TcpCommand.Pong, b'')
    )

    assert reply.action == ReplyAction.CompleteScalar
    assert reply.result is True
    assert reply.next_message is None
