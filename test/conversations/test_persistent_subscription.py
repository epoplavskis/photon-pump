from typing import NamedTuple
from uuid import UUID, uuid4

import pytest

from photonpump import exceptions as exn
from photonpump import messages_pb2 as proto
from photonpump.conversations import (
    ConnectPersistentSubscription, PersistentSubscription
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


async def confirm_subscription(
        convo,
        commit=23,
        event_number=56,
        subscription_id='FUUBARRBAXX',
        queue=None
):

    response = proto.PersistentSubscriptionConfirmation()
    response.last_commit_position = commit
    response.last_event_number = event_number
    response.subscription_id = subscription_id

    await convo.respond_to(
        InboundMessage(
            convo.conversation_id,
            TcpCommand.PersistentSubscriptionConfirmation,
            response.SerializeToString()
        ), queue
    )


@pytest.mark.asyncio
async def test_confirmation():
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )

    await confirm_subscription(
        convo, subscription_id='my-subscription', event_number=10
    )

    subscription = convo.result.result()

    assert subscription.name == 'my-subscription'
    assert subscription.stream == 'my-stream'
    assert subscription.last_event_number == 10


@pytest.mark.asyncio
async def test_dropped_on_connect():
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )

    with pytest.raises(exn.SubscriptionCreationFailed):
        await drop_subscription(convo, SubscriptionDropReason.Unsubscribed)
        await convo.result


def event_appeared(event_id):
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

    return response


@pytest.mark.asyncio
async def test_stream_event_appeared():
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )

    event_id = uuid4()
    await confirm_subscription(convo)
    response = event_appeared(event_id)

    await convo.respond_to(
        InboundMessage(
            uuid4(), TcpCommand.PersistentSubscriptionStreamEventAppeared,
            response.SerializeToString()
        ), None
    )

    subscription = await convo.result
    event = await subscription.events.anext()

    assert event.event.id == event_id


@pytest.mark.asyncio
async def test_subscription_unsubscribed_midway():
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )
    await confirm_subscription(convo)
    subscription = await convo.result

    await drop_subscription(convo, SubscriptionDropReason.Unsubscribed)
    with pytest.raises(StopAsyncIteration):
        await subscription.events.anext()


@pytest.mark.asyncio
async def test_subscription_failed_midway():
    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )
    await confirm_subscription(convo)
    subscription = await convo.result

    await drop_subscription(convo, SubscriptionDropReason.AccessDenied)
    with pytest.raises(exn.SubscriptionFailed):
        await subscription.events.anext()


class stub_event(NamedTuple):
    original_event_id: UUID


@pytest.mark.asyncio
async def test_acknowledge_event():
    output = TeeQueue()

    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )

    await confirm_subscription(convo, subscription_id=convo.name, queue=output)
    event_id = uuid4()

    subscription = await convo.result
    await subscription.ack(stub_event(event_id))

    ack = await output.get()

    assert ack.command == TcpCommand.PersistentSubscriptionAckEvents
    assert ack.conversation_id == convo.conversation_id

    expected_payload = proto.PersistentSubscriptionAckEvents()
    expected_payload.subscription_id = convo.name
    expected_payload.processed_event_ids.append(event_id.bytes_le)

    assert expected_payload.SerializeToString() == ack.payload


@pytest.mark.asyncio
async def test_use_new_output_when_reconnected():
    first_output = TeeQueue()
    second_output = TeeQueue()

    convo = ConnectPersistentSubscription(
        'my-subscription', 'my-stream', max_in_flight=57
    )

    await confirm_subscription(convo, subscription_id=convo.name, queue=first_output)
    event_id = uuid4()

    subscription = await convo.result

    await confirm_subscription(convo, subscription_id=convo.name, queue=second_output)
    await subscription.ack(stub_event(event_id))

    ack = await second_output.get()

    assert ack.command == TcpCommand.PersistentSubscriptionAckEvents
    assert ack.conversation_id == convo.conversation_id

    assert not first_output.items


