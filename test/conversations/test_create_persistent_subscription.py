from uuid import uuid4

from photonpump import messages_pb2 as proto
from photonpump import exceptions
from photonpump.conversations import CreatePersistentSubscription, ReplyAction
from photonpump.messages import InboundMessage, SubscriptionResult, TcpCommand


def test_create_persistent_subscription_request():

    convo = CreatePersistentSubscription("my-subscription", "my-stream")
    request = convo.start()

    body = proto.CreatePersistentSubscription()
    body.ParseFromString(request.payload)

    assert request.command == TcpCommand.CreatePersistentSubscription
    assert body.subscription_group_name == 'my-subscription'
    assert body.event_stream_id == 'my-stream'


def complete_subscription(convo, result):
    response = proto.CreatePersistentSubscriptionCompleted()
    response.result = result

    return convo.respond_to(
        InboundMessage(
            uuid4(), TcpCommand.CreatePersistentSubscriptionCompleted,
            response.SerializeToString()
        )
    )


def test_persistent_subscription_completed():

    convo = CreatePersistentSubscription(
        "my-other-subscription", "my-other-stream"
    )

    reply = complete_subscription(convo, SubscriptionResult.Success)
    assert reply.action == ReplyAction.CompleteScalar
    assert reply.next_message is None
    assert reply.result is None


def test_persistent_subscription_already_exists():

    convo = CreatePersistentSubscription(
        "my-other-subscription", "my-other-stream"
    )

    convo.start()

    response = proto.CreatePersistentSubscriptionCompleted()
    response.result = SubscriptionResult.AlreadyExists
    reply = convo.respond_to(
        InboundMessage(
            uuid4(), TcpCommand.CreatePersistentSubscriptionCompleted,
            response.SerializeToString()
        )
    )

    assert reply.action == ReplyAction.CompleteError
    assert reply.next_message is None
    assert isinstance(reply.result, exceptions.SubscriptionCreationFailed)


def test_persistent_subscription_access_denied():

    convo = CreatePersistentSubscription(
        "my-other-subscription", "my-other-stream"
    )

    reply = complete_subscription(convo, SubscriptionResult.AccessDenied)
    assert reply.action == ReplyAction.CompleteError
    assert reply.next_message is None
    exn = reply.result

    assert isinstance(exn, exceptions.AccessDenied)
