from uuid import uuid4

from photonpump import messages_pb2 as proto
from photonpump.conversations import CreateVolatileSubscription, ReplyAction
from photonpump.messages import (
    ContentType, Event, InboundMessage, SubscriptionDropReason,
    TcpCommand
)


def test_create_volatile_subscription_request():

    convo = CreateVolatileSubscription('my-stream')
    request = convo.start()

    assert request.command == TcpCommand.SubscribeToStream
    body = proto.SubscribeToStream()
    body.ParseFromString(request.payload)

    assert body.event_stream_id == 'my-stream'
    assert body.resolve_link_tos is True


def confirm_subscription(convo):
    response = proto.SubscriptionConfirmation()
    response.last_commit_position = 23
    response.last_event_number = 34

    return convo.respond_to(
        InboundMessage(
            uuid4(), TcpCommand.SubscriptionConfirmation,
            response.SerializeToString()
        )
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


def test_subscription_confirmed():

    convo = CreateVolatileSubscription('eds-stream')
    convo.start()

    reply = confirm_subscription(convo)

    assert reply.action == ReplyAction.BeginVolatileSubscription
    assert reply.result.last_commit_position == 23
    assert reply.result.last_event_number == 34
    assert reply.result.stream == 'eds-stream'


def test_stream_event_appeared():

    convo = CreateVolatileSubscription('my-stream')
    confirm_subscription(convo)

    event_id = uuid4()
    response = proto.StreamEventAppeared()

    response.event.commit_position = 0
    response.event.prepare_position = 1
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
            uuid4(), TcpCommand.StreamEventAppeared,
            response.SerializeToString()
        )
    )

    assert reply.action == ReplyAction.YieldToSubscription
    assert isinstance(reply.result, Event)
    assert reply.result.event.id == event_id

    assert reply.next_message is None


def test_subscription_dropped():

    convo = CreateVolatileSubscription('my-stream')
    confirm_subscription(convo)

    reply = drop_subscription(convo, SubscriptionDropReason.Unsubscribed)
    assert reply.action == ReplyAction.FinishSubscription
