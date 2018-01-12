from uuid import uuid4

from photonpump import messages_pb2 as proto
from photonpump import exceptions
from photonpump.conversations import (
    ConnectPersistentSubscription, PersistentSubscription, ReplyAction
)
from photonpump.messages import (
    ContentType, Event, InboundMessage, SubscriptionDropReason,
    SubscriptionResult, TcpCommand
)


def drop_subscription(convo, reason=SubscriptionDropReason):

    response = proto.SubscriptionDropped()
    response.reason = reason

    return convo.respond_to(
        InboundMessage(
            uuid4(), TcpCommand.SubscriptionDropped,
            response.SerializeToString()
        )
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


def confirm_subscription(convo):

    response = proto.PersistentSubscriptionConfirmation()
    response.last_commit_position = 23
    response.last_event_number = 56
    response.subscription_id = 'FUURBARRBAXX'

    return convo.respond_to(
        InboundMessage(
            convo.conversation_id,
            TcpCommand.PersistentSubscriptionConfirmation,
            response.SerializeToString()
        )
    )


def test_confirmation():
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )

    reply = confirm_subscription(convo)

    assert reply.action == ReplyAction.BeginPersistentSubscription
    assert isinstance(reply.result, PersistentSubscription)


def test_dropped_on_connect():
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )
    reply = drop_subscription(convo, SubscriptionDropReason.Unsubscribed)
    assert reply.action == ReplyAction.CompleteError


def test_stream_event_appeared():
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )

    confirm_subscription(convo)

    event_id = uuid4()
    response = proto.PersistentSubscriptionStreamEventAppeared()

    response.event.event.event_stream_id = "stream-123"
    response.event.event.event_number = 32
    response.event.event.event_id = event_id.bytes_le
    response.event.event.event_type = 'event-type'
    response.event.event.data_content_type = ContentType.Json
    response.event.event.metadata_content_type = ContentType.Binary
    response.event.event.data = """
    {
        'color': 'blue',
        'winner': false
    }
    """.encode('UTF-8')

    reply = convo.respond_to(
        InboundMessage(
            uuid4(), TcpCommand.PersistentSubscriptionStreamEventAppeared,
            response.SerializeToString()
        )
    )

    assert reply.action == ReplyAction.YieldToSubscription
    assert isinstance(reply.result, Event)
    assert reply.result.event.id == event_id

    assert reply.next_message is None
