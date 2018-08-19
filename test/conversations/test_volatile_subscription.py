import asyncio
from uuid import uuid4
from photonpump import exceptions as exn

import pytest

from photonpump import messages_pb2 as proto
from photonpump.conversations import SubscribeToStream
from photonpump.messages import (
    ContentType,
    InboundMessage,
    SubscriptionDropReason,
    TcpCommand,
)

from ..fakes import TeeQueue


async def anext(it):
    return await asyncio.wait_for(it.anext(), 1)


async def respond_to(convo, message, queue=None):
    await convo.respond_to(message, queue)


async def drop_subscription(convo, reason=SubscriptionDropReason.Unsubscribed):

    response = proto.SubscriptionDropped()
    response.reason = reason

    await convo.respond_to(
        InboundMessage(
            uuid4(), TcpCommand.SubscriptionDropped, response.SerializeToString()
        ),
        None,
    )


async def confirm_subscription(convo, event_number=1, commit_pos=1):

    response = proto.SubscriptionConfirmation()
    response.last_event_number = event_number
    response.last_commit_position = commit_pos

    await convo.respond_to(
        InboundMessage(
            uuid4(), TcpCommand.SubscriptionConfirmation, response.SerializeToString()
        ),
        None,
    )


def event_appeared(event_id, commit_position=1, prepare_position=1):
    message_id = uuid4()
    response = proto.StreamEventAppeared()

    response.event.event.event_stream_id = "stream-123"
    response.event.event.event_number = 32
    response.event.event.event_id = event_id.bytes_le
    response.event.event.event_type = "event-type"
    response.event.event.data_content_type = ContentType.Json
    response.event.event.metadata_content_type = ContentType.Binary
    response.event.commit_position = commit_position
    response.event.prepare_position = prepare_position
    response.event.event.data = """
    {
        'color': 'blue',
        'winner': false
    }
    """.encode(
        "UTF-8"
    )

    return InboundMessage(
        message_id, TcpCommand.StreamEventAppeared, response.SerializeToString()
    )


@pytest.mark.asyncio
async def test_subscribe_to_stream():

    output = TeeQueue()
    convo = SubscribeToStream("my-stream")
    await convo.start(output)
    [request] = output.items

    payload = proto.SubscribeToStream()
    payload.ParseFromString(request.payload)

    assert request.command == TcpCommand.SubscribeToStream
    assert payload.event_stream_id == "my-stream"
    assert payload.resolve_link_tos is True


@pytest.mark.asyncio
async def test_confirmation():
    convo = SubscribeToStream("my-stream")

    await confirm_subscription(convo, event_number=10, commit_pos=10)

    subscription = convo.result.result()

    assert subscription.stream == "my-stream"
    assert subscription.last_event_number == 10
    assert subscription.last_commit_position == 10
    assert subscription.first_event_number == 10
    assert subscription.first_commit_position == 10


@pytest.mark.asyncio
async def test_event_appeared():
    convo = SubscribeToStream("my-stream")
    event_id = uuid4()

    await confirm_subscription(convo, event_number=10, commit_pos=10)
    subscription = convo.result.result()

    await respond_to(convo, event_appeared(event_id))

    event = await subscription.events.anext()
    assert event.id == event_id


@pytest.mark.asyncio
async def test_subscription_dropped_mid_stream():
    convo = SubscribeToStream("my-stream")
    event_id = uuid4()

    await confirm_subscription(convo, event_number=10, commit_pos=10)
    subscription = convo.result.result()

    await respond_to(convo, event_appeared(event_id))
    await drop_subscription(convo)

    events = [e async for e in subscription.events]
    assert len(events) == 1


@pytest.mark.asyncio
async def test_subscription_failure_mid_stream():
    convo = SubscribeToStream("my-stream")
    event_id = uuid4()

    await confirm_subscription(convo, event_number=10, commit_pos=10)
    subscription = convo.result.result()

    await respond_to(convo, event_appeared(event_id))
    await drop_subscription(convo, SubscriptionDropReason.SubscriberMaxCountReached)

    with pytest.raises(exn.SubscriptionFailed):
        event = await anext(subscription.events)
        assert event.id == event_id

        await anext(subscription.events)


@pytest.mark.asyncio
async def test_failure_on_subscribe():
    convo = SubscribeToStream("my-stream")

    await drop_subscription(convo, SubscriptionDropReason.SubscriberMaxCountReached)

    with pytest.raises(exn.SubscriptionCreationFailed):
        await convo.result
