from uuid import uuid4

from photonpump import messages_pb2 as proto
from photonpump import exceptions
from photonpump.conversations import ConnectPersistentSubscription, ReplyAction
from photonpump.messages import InboundMessage, SubscriptionResult, TcpCommand


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
