from uuid import uuid4

from photonpump import messages_pb2 as proto
from photonpump import exceptions
from photonpump.conversations import ConnectPersistentSubscription, PersistentSubscription, ReplyAction
from photonpump.messages import (
    InboundMessage, SubscriptionResult, TcpCommand
)


def test_connect_request():

    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )
    request = convo.start()

    payload = proto.ConnectToPersistentSubscription()
    payload.ParseFromString(request.payload)

    assert request.command == TcpCommand.ConnectToPersistentSubscription
    assert payload.subscription_id == 'my-subscription'
    assert payload.event_stream_id == 'my-stream'
    assert payload.allowed_in_flight_messages == 57


def test_confirmation():
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )

    response = proto.PersistentSubscriptionConfirmation()
    response.last_commit_position = 23
    response.last_event_number = 56
    response.subscription_id = 'FUURBARRBAXX'

    reply = convo.respond_to(
        InboundMessage(
            convo.conversation_id,
            TcpCommand.PersistentSubscriptionConfirmation,
            response.SerializeToString()
        )
    )

    assert reply.action == ReplyAction.BeginPersistentSubscription
    assert isinstance(reply.result, PersistentSubscription)
