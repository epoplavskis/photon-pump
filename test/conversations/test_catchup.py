import asyncio
import json
import pytest
import uuid
from ..fakes import TeeQueue
from photonpump.conversations import CatchupSubscription
from photonpump import messages as msg
from photonpump import messages_pb2 as proto


async def anext(it):
    return await asyncio.wait_for(it.anext(), 1)


async def reply_to(convo, message, output):
    print(message)
    command, payload = message
    await convo.respond_to(msg.InboundMessage(uuid.uuid4(), command, payload), output)


async def drop_subscription(convo, reason=msg.SubscriptionDropReason.Unsubscribed):

    response = proto.SubscriptionDropped()
    response.reason = reason

    await convo.respond_to(
        msg.InboundMessage(
            uuid.uuid4(), msg.TcpCommand.SubscriptionDropped, response.SerializeToString()
        ),
        None,
    )




async def confirm_subscription(convo, output_queue=None, event_number=1, commit_pos=1):

    response = proto.SubscriptionConfirmation()
    response.last_event_number = event_number
    response.last_commit_position = commit_pos

    await convo.respond_to(
        msg.InboundMessage(
            uuid.uuid4(),
            msg.TcpCommand.SubscriptionConfirmation,
            response.SerializeToString(),
        ),
        output_queue,
    )

    return await convo.result


def event_appeared(
    commit_position=1,
    prepare_position=1,
    event_number=10,
    event_id=None,
    type="some-event",
    data=None,
    stream="stream-123",
):
    response = proto.StreamEventAppeared()

    response.event.event.event_stream_id = stream
    response.event.event.event_number = event_number
    response.event.event.event_id = (event_id or uuid.uuid4()).bytes_le
    response.event.event.event_type = type
    response.event.event.data_content_type = msg.ContentType.Json
    response.event.event.metadata_content_type = msg.ContentType.Binary
    response.event.commit_position = commit_position
    response.event.prepare_position = prepare_position
    response.event.event.data = json.dumps(data).encode("UTF-8") if data else bytes()

    return (msg.TcpCommand.StreamEventAppeared, response.SerializeToString())


class ReadStreamEventsResponseBuilder:
    def __init__(self, stream=None):
        self.result = msg.ReadStreamResult.Success
        self.next_event_number = 10
        self.last_event_number = 9
        self.is_end_of_stream = False
        self.last_commit_position = 8
        self.stream = stream or "some-stream"
        self.events = []

    def at_end_of_stream(self):
        self.is_end_of_stream = True

        return self

    def with_next_event_number(self, num):
        self.next_event_number = num

        return self

    def with_last_position(self, event_number=9, commit_position=8):
        self.last_event_number = event_number
        self.last_commit_position = commit_position

        return self

    def with_event(self, event_number=10, event_id=None, type="some-event", data=None):
        event = proto.ResolvedIndexedEvent()
        event.event.event_stream_id = self.stream
        event.event.event_number = event_number
        event.event.event_id = (event_id or uuid.uuid4()).bytes_le
        event.event.event_type = type
        event.event.data_content_type = msg.ContentType.Json
        event.event.metadata_content_type = msg.ContentType.Binary
        event.event.data = json.dumps(data).encode("UTF-8") if data else bytes()

        self.events.append(event)

        return self

    def build(self):
        response = proto.ReadStreamEventsCompleted()
        response.result = self.result
        response.next_event_number = self.next_event_number
        response.last_event_number = self.last_event_number
        response.is_end_of_stream = self.is_end_of_stream
        response.last_commit_position = self.last_commit_position

        response.events.extend(self.events)

        return msg.TcpCommand.ReadStreamEventsForwardCompleted, response.SerializeToString()


@pytest.mark.asyncio
async def test_start_read_phase():
    """
    A "catchup" subscription starts by iterating the events in the stream until
    it reaches the most recent event.

    This is the "Read" phase.
    """

    output = TeeQueue()

    conversation_id = uuid.uuid4()
    convo = CatchupSubscription("my-stream", conversation_id=conversation_id)

    await convo.start(output)
    [request] = output.items

    body = proto.ReadStreamEvents()
    body.ParseFromString(request.payload)

    assert request.command is msg.TcpCommand.ReadStreamEventsForward
    assert body.event_stream_id == "my-stream"
    assert body.from_event_number == 0
    assert body.resolve_link_tos is True
    assert body.require_master is False
    assert body.max_count == 100


@pytest.mark.asyncio
async def test_end_of_stream():
    """
    During the Read phase, we yield the events to the subscription so that the
    user is unaware of the chicanery in the background.

    When we reach the end of the stream, we should send a subscribe message to
    start the volatile subscription.
    """

    convo = CatchupSubscription("my-stream")
    output = TeeQueue()
    await convo.start(output)

    event_1_id = uuid.uuid4()
    event_2_id = uuid.uuid4()

    response = (
        ReadStreamEventsResponseBuilder(stream="stream-123")
        .at_end_of_stream()
        .with_event(event_id=event_1_id, event_number=32)
        .with_event(event_id=event_2_id, event_number=33)
    ).build()

    await reply_to(convo, response, output)

    subscription = await convo.result

    event_1 = await anext(subscription.events)
    event_2 = await anext(subscription.events)

    assert event_1.stream == "stream-123"
    assert event_1.id == event_1_id
    assert event_1.event_number == 32

    assert event_2.stream == "stream-123"
    assert event_2.id == event_2_id
    assert event_2.event_number == 33


@pytest.mark.asyncio
async def test_paging():
    """
    During the read phase, we expect to page through multiple batches of
    events. In this scenario we have two batches, each of two events.
    """

    convo = CatchupSubscription("my-stream")
    output = TeeQueue()
    await convo.start(output)
    await output.get()

    event_1_id = uuid.uuid4()
    event_2_id = uuid.uuid4()
    event_3_id = uuid.uuid4()
    event_4_id = uuid.uuid4()

    first_response = (
        ReadStreamEventsResponseBuilder()
        .with_event(event_id=event_1_id, event_number=32)
        .with_event(event_id=event_2_id, event_number=33)
        .with_next_event_number(34)
    ).build()

    second_response = (
        ReadStreamEventsResponseBuilder()
        .with_event(event_id=event_3_id, event_number=34)
        .with_event(event_id=event_4_id, event_number=35)
    ).build()

    await reply_to(convo, first_response, output)
    subscription = await convo.result

    event_1 = await anext(subscription.events)
    event_2 = await anext(subscription.events)
    assert event_1.id == event_1_id
    assert event_2.id == event_2_id

    reply = await output.get()
    body = proto.ReadStreamEvents()
    body.ParseFromString(reply.payload)
    assert body.from_event_number == 34

    await reply_to(convo, second_response, output)

    event_3 = await anext(subscription.events)
    event_4 = await anext(subscription.events)
    assert event_3.id == event_3_id
    assert event_4.id == event_4_id


@pytest.mark.asyncio
async def test_subscribes_at_end_of_stream():

    """
    When we have read all the events in the stream, we should send a
    request to subscribe for new events.
    """

    convo = CatchupSubscription("my-stream")
    output = TeeQueue()
    await convo.start(output)
    await output.get()

    await reply_to(
        convo,
        ReadStreamEventsResponseBuilder().at_end_of_stream().build(),
        output,
    )

    reply = await output.get()
    payload = proto.SubscribeToStream()
    payload.ParseFromString(reply.payload)

    assert reply.command == msg.TcpCommand.SubscribeToStream
    assert payload.event_stream_id == "my-stream"
    assert payload.resolve_link_tos is True


@pytest.mark.asyncio
async def test_should_perform_a_catchup_when_subscription_is_confirmed():

    """
    When we have read all the events in the stream, we should send a
    request to subscribe for new events.
    """

    convo = CatchupSubscription("my-stream")
    output = TeeQueue()
    await convo.start(output)

    await reply_to(
        convo,
        ReadStreamEventsResponseBuilder().at_end_of_stream().build(),
        output,
    )
    await confirm_subscription(convo, output, event_number=42, commit_pos=40)
    [read_historial, subscribe, catch_up] = await output.next_event(3)

    assert read_historial.command == msg.TcpCommand.ReadStreamEventsForward
    assert subscribe.command == msg.TcpCommand.SubscribeToStream
    assert catch_up.command == msg.TcpCommand.ReadStreamEventsForward

    payload = proto.ReadStreamEvents()
    payload.ParseFromString(catch_up.payload)

    assert payload.event_stream_id == "my-stream"
    assert payload.from_event_number == 42


@pytest.mark.asyncio
async def test_should_return_catchup_events_before_subscribed_events():

    """
    It's possible that the following sequence of events occurs:
        * The client reads the last batch of events from a stream containing
          50 events.
        * The client sends SubscribeToStream
        * Event 51 is written to the stream
        * The server creates a subscription starting at event 51 and
          responds with SubscriptionConfirmed
        * Event 52 is written to the stream
        * The client receives event 52.

    To solve this problem, the client needs to perform an additional read
    to fetch any missing events created between the last batch and the
    subscription confirmation.

    --------------

    In this scenario, we read a single event (1) from the end of the stream
    and expect to create a subscription.

    We receive event 4 immediately on the subscription. We expect that the
    client requests missing events.

    We receive two pages, of one event each: 3, and 4, and then drop the subscription.

    Lastly, we expect that the events are yielded in the correct order
    despite being received out of order and that we have no duplicates.
    """

    convo = CatchupSubscription("my-stream")
    output = TeeQueue()
    await convo.start(output)
    await output.get()

    last_page = (
        ReadStreamEventsResponseBuilder()
        .at_end_of_stream()
        .with_event(event_number=1, type="a")
        .build()
    )

    subscribed_event = event_appeared(event_number=4, type="d")

    first_catchup = ReadStreamEventsResponseBuilder().with_event(
        event_number=2, type="b"
    ).build()
    second_catchup = (
        ReadStreamEventsResponseBuilder()
        .with_event(event_number=3, type="c")
        .with_event(event_number=4, type="d")
        .at_end_of_stream()
    ).build()

    await reply_to(convo, last_page, output)
    assert (await output.get()).command == msg.TcpCommand.SubscribeToStream

    await confirm_subscription(convo, output, event_number=3)
    await reply_to(convo, subscribed_event, output)
    assert (await output.get()).command == msg.TcpCommand.ReadStreamEventsForward

    await reply_to(convo, first_catchup, output)
    assert (await output.get()).command == msg.TcpCommand.ReadStreamEventsForward

    await reply_to(convo, second_catchup, output)
    await drop_subscription(convo)

    events = []
    subscription = await convo.result
    async for e in subscription.events:
        events.append(e)

    assert len(events) == 4
    [a, b, c, d] = events

    assert a.event_number == 1
    assert b.event_number == 2
    assert c.event_number == 3
    assert d.event_number == 4
