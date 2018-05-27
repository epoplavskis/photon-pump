from uuid import uuid4

import pytest

from photonpump import messages_pb2 as proto
from photonpump import exceptions as exn
from photonpump.conversations import (
    ConnectPersistentSubscription, PersistentSubscription, ReplyAction
)
from photonpump.messages import (
    ContentType, Event, InboundMessage, SubscriptionDropReason, TcpCommand
)
from ..fakes import TeeQueue


async def drop_subscription(convo, reason=SubscriptionDropReason):

    response = proto.SubscriptionDropped()
    response.reason = reason

    await convo.respond_to(
        InboundMessage(
            uuid4(), TcpCommand.SubscriptionDropped,
            response.SerializeToString()
        ), None
    )


@pytest.mark.asyncio
async def test_connect_request():

    output = TeeQueue()
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )
    await convo.start(output)
    [request] = output.items

    payload = proto.ConnectToPersistentSubscription()
    payload.ParseFromString(request.payload)

    assert request.command == TcpCommand.ConnectToPersistentSubscription
    assert payload.subscription_id == 'my-subscription'
    assert payload.event_stream_id == 'my-stream'
    assert payload.allowed_in_flight_messages == 57


async def confirm_subscription(convo, commit=23, event_number=56, subscription_id='FUUBARRBAXX'):

    response = proto.PersistentSubscriptionConfirmation()
    response.last_commit_position = commit
    response.last_event_number = event_number
    response.subscription_id = subscription_id

    await convo.respond_to(
        InboundMessage(
            convo.conversation_id,
            TcpCommand.PersistentSubscriptionConfirmation,
            response.SerializeToString()
        ), None
    )


@pytest.mark.asyncio
async def test_confirmation():
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )

    await confirm_subscription(convo, subscription_id='my-subscription', event_number=10)

    subscription = convo.result.result()

    assert subscription.name == 'my-subscription'
    assert subscription.stream == 'my-stream'
    assert subscription.last_event_number == 10


def test_dropped_on_connect():
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )
    reply = drop_subscription(convo, SubscriptionDropReason.Unsubscribed)
    assert reply.action == ReplyAction.CompleteError


def test_persistent_subscription_reconnection():
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )

    confirm_subscription(convo)
    reply = confirm_subscription(convo)

    assert reply.action == ReplyAction.ContinueSubscription


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


def test_subscription_unsubscribed_midway():
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )
    confirm_subscription(convo)
    reply = drop_subscription(convo, SubscriptionDropReason.Unsubscribed)
    assert reply.action == ReplyAction.FinishSubscription


def test_subscription_failed_midway():
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )
    confirm_subscription(convo)
    reply = drop_subscription(convo, SubscriptionDropReason.AccessDenied)
    assert reply.action == ReplyAction.RaiseToSubscription
    assert isinstance(reply.result, exn.SubscriptionFailed)
