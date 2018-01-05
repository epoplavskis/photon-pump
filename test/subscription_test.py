from asyncio import Future
from uuid import uuid4

import pytest
from photonpump import messages_pb2 as proto
from photonpump import (
    CreateVolatileSubscription, Header, OperationFlags, TcpCommand, parse_header
)


class FakeWriter:

    def __init__(self):
        self.chunks = []

    def write(self, chunk):
        self.chunks.append(chunk)

    def header(self):
        length = self.chunks[0][0:4]
        data = self.chunks[0][4:]

        return parse_header(length, data)

    def body(self, cls):
        result = cls()
        result.ParseFromString(self.chunks[1])

        return result


@pytest.mark.asyncio
async def test_connection_message(event_loop):
    correlation_id = uuid4()
    subscription = CreateVolatileSubscription(
        'my-stream', correlation_id=correlation_id
    )

    writer = FakeWriter()
    subscription.send(writer)

    assert writer.header() == Header(
        31, TcpCommand.SubscribeToStream, OperationFlags.Empty, correlation_id
    )

    cmd = writer.body(proto.SubscribeToStream)
    assert cmd.event_stream_id == "my-stream"
    assert cmd.resolve_link_tos == True


async def confirm_subscription(subscription, commit_pos, event_number):
    header = Header(
        0, TcpCommand.SubscriptionConfirmation.value, OperationFlags.Empty,
        uuid4()
    )
    payload = proto.SubscriptionConfirmation()
    payload.last_commit_position = commit_pos
    payload.last_event_number = event_number

    await subscription.handle_response(
        header, payload.SerializeToString(), None
    )


async def receive_event(
        subscription, commit_pos=0, event_number=0, event_type='fake-news'
):
    record = proto.EventRecord()
    record.event_stream_id = 'my-stream'
    record.event_id = uuid4().bytes
    record.event_number = event_number
    record.event_type = event_type
    record.data_content_type = 0
    record.metadata_content_type = 0
    record.data = bytes()

    header = Header(
        0, TcpCommand.StreamEventAppeared.value, OperationFlags.Empty, uuid4()
    )

    event = proto.ResolvedEvent()
    event.event.CopyFrom(record)
    event.commit_position = commit_pos
    event.prepare_position = -1

    payload = proto.StreamEventAppeared()
    payload.event.CopyFrom(event)

    await subscription.handle_response(
        header, payload.SerializeToString(), None
    )


async def drop_subscription(subscription):

    header = Header(
        0, TcpCommand.SubscriptionDropped.value, OperationFlags.Empty, uuid4()
    )

    payload = proto.SubscriptionDropped()
    await subscription.handle_response(
        header, payload.SerializeToString(), None
    )


@pytest.mark.asyncio
async def test_subscription_confirmed(event_loop):
    cmd = CreateVolatileSubscription('my-stream', correlation_id=uuid4())

    assert cmd.future.done() is False

    await confirm_subscription(cmd, 23, 34)

    subscription = cmd.future.result()
    assert subscription.last_commit_position == 23
    assert subscription.last_event_number == 34


@pytest.mark.asyncio
async def test_event_appeared(event_loop):
    cmd = CreateVolatileSubscription('my-stream', correlation_id=uuid4())

    await confirm_subscription(cmd, 1, 2)

    subscription = cmd.future.result()
    assert subscription.last_commit_position == 1
    assert subscription.last_event_number == 2

    await receive_event(
        cmd, event_type='fake-news', commit_pos=50, event_number=32
    )
    event = await subscription.events.__anext__()

    assert event.type == 'fake-news'
    assert subscription.last_commit_position == 50
    assert subscription.last_event_number == 32


@pytest.mark.asyncio
async def test_subscription_dropped(event_loop):
    cmd = CreateVolatileSubscription('my-stream', correlation_id=uuid4(), buffer_size=4)

    await confirm_subscription(cmd, 1, 2)
    print(1)
    await receive_event(
        cmd, event_type='fake-news', commit_pos=51, event_number=33
    )
    print(1)
    await receive_event(
        cmd, event_type='fake-news', commit_pos=52, event_number=34
    )
    print(1)
    await receive_event(
        cmd, event_type='fake-news', commit_pos=53, event_number=35
    )

    print(1)
    await drop_subscription(cmd)

    print(1)
    subscription = await cmd.future
    events = [e async for e in subscription.events]

    assert len(events) == 3
    assert events[2].event_number == 35
