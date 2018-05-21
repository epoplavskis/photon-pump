import binascii
from uuid import UUID, uuid4

from photonpump import exceptions
from photonpump import messages_pb2 as proto
from photonpump.conversations import CreatePersistentSubscription, ReplyAction
from photonpump.messages import (
    InboundMessage, SubscriptionResult, TcpCommand
)


def read_hex(s):
    return binascii.unhexlify(''.join(s.split()))


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


def test_subscription_configuration():
    convo = CreatePersistentSubscription(
        name="best-subscription",
        stream="$ce-Cancellation",
        resolve_links=True,
        start_from=0,
        timeout_ms=30000,
        record_statistics=False,
        live_buffer_size=32,
        read_batch_size=64,
        buffer_size=128,
        max_retry_count=10,
        prefer_round_robin=True,
        checkpoint_after_ms=2000,
        checkpoint_max_count=1000,
        checkpoint_min_count=10,
        subscriber_max_count=0,
        conversation_id=UUID('8ff5727b-58a9-4805-823a-451d5eb307f7')
    )

    request = convo.start()

    expected_bytes = read_hex(
        """
C8 00 7B 72 F5 8F A9 58 05 48 82 3A 45 1D 5E B3
07 F7 0A 11 62 65 73 74 2D 73 75 62 73 63 72 69
70 74 69 6F 6E 12 10 24 63 65 2D 43 61 6E 63 65
6C 6C 61 74 69 6F 6E 18 01 20 00 28 B0 EA 01 30
00 38 20 40 40 48 80 01 50 0A 58 01 60 D0 0F 68
E8 07 70 0A 78 00 82 01 0A 52 6F 75 6E 64 52 6F
62 69 6E
"""
    )

    actual_bytes = (request.header_bytes + request.payload)[4:]

    assert actual_bytes == expected_bytes
    assert len(actual_bytes) == len(expected_bytes)
