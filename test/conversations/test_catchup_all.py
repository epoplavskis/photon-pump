import asyncio
import json
import uuid

import pytest
from photonpump import exceptions as exn
from photonpump import messages as msg
from photonpump import messages_pb2 as proto
from photonpump.conversations import CatchupAllSubscription

from ..fakes import TeeQueue


async def anext(it, count=1):
    if count == 1:
        return await asyncio.wait_for(it.anext(), 1)
    result = []

    while len(result) < count:
        result.append(await asyncio.wait_for(it.anext(), 1))

    return result


async def reply_to(convo, message, output):
    command, payload = message
    await convo.respond_to(msg.InboundMessage(uuid.uuid4(), command, payload), output)


def read_as(cls, message):
    body = cls()
    body.ParseFromString(message.payload)

    return body


async def drop_subscription(
    convo, output, reason=msg.SubscriptionDropReason.Unsubscribed
):

    response = proto.SubscriptionDropped()
    response.reason = reason

    await convo.respond_to(
        msg.InboundMessage(
            uuid.uuid4(),
            msg.TcpCommand.SubscriptionDropped,
            response.SerializeToString(),
        ),
        output,
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
    commit_position=None,
    prepare_position=None,
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
    response.event.commit_position = commit_position or event_number
    response.event.prepare_position = prepare_position or event_number
    response.event.event.data = json.dumps(data).encode("UTF-8") if data else bytes()

    return (msg.TcpCommand.StreamEventAppeared, response.SerializeToString())


class ReadAllEventsResponseBuilder:
    def __init__(self):
        self.result = msg.ReadAllResult.Success
        self.next_position = msg.Position(10, 10)
        self.position = msg.Position(9, 9)
        self.is_end_of_stream = False
        self.last_commit_position = 8
        self.events = []

    def with_next_position(self, commit=0, prepare=0):
        self.next_position = msg.Position(commit, prepare)

        return self

    def at_end_of_stream(self, commit=0, prepare=0):
        self.with_position(commit, prepare)
        self.with_next_position(commit, prepare)
        return self

    def with_position(self, commit=0, prepare=0):
        self.position = msg.Position(commit, prepare)

        return self

    def with_event(
        self,
        commit=None,
        prepare=None,
        event_number=10,
        event_id=None,
        type="some-event",
        data=None,
        link_event_number=None,
    ):
        event = proto.ResolvedEvent()
        event.commit_position = commit or event_number
        event.prepare_position = prepare or event_number
        event.event.event_stream_id = "some-stream-name"
        event.event.event_number = event_number
        event.event.event_id = (event_id or uuid.uuid4()).bytes_le
        event.event.event_type = type
        event.event.data_content_type = msg.ContentType.Json
        event.event.metadata_content_type = msg.ContentType.Binary
        event.event.data = json.dumps(data).encode("UTF-8") if data else bytes()
        if link_event_number is not None:
            event.link.event_number = link_event_number
            event.link.event_stream_id = "some-stream-name"
            event.link.event_id = uuid.uuid4().bytes_le
            event.link.event_type = "$>"
            event.link.data_content_type = msg.ContentType.Json
            event.link.metadata_content_type = msg.ContentType.Binary
            event.link.data = f"{event_number}@{self.stream}".encode("UTF-8")

        self.events.append(event)

        return self

    def build(self):
        response = proto.ReadAllEventsCompleted()
        response.result = self.result
        response.commit_position = self.position.commit
        response.prepare_position = self.position.prepare
        response.next_commit_position = self.next_position.commit
        response.next_prepare_position = self.next_position.prepare

        response.events.extend(self.events)

        return (
            msg.TcpCommand.ReadAllEventsForwardCompleted,
            response.SerializeToString(),
        )


EMPTY_STREAM_PAGE = ReadAllEventsResponseBuilder().at_end_of_stream().build()


@pytest.mark.asyncio
async def test_start_read_phase():
    """
    A "catchup" subscription starts by iterating the events in the stream until
    it reaches the most recent event.

    This is the "Read" phase.
    """

    output = TeeQueue()

    conversation_id = uuid.uuid4()
    convo = CatchupAllSubscription(
        start_from=msg.Position(0, 0), conversation_id=conversation_id
    )

    await convo.start(output)
    [request] = output.items

    body = proto.ReadAllEvents()
    body.ParseFromString(request.payload)

    assert request.command is msg.TcpCommand.ReadAllEventsForward
    assert body.commit_position == 0
    assert body.prepare_position == 0
    assert body.resolve_link_tos is True
    assert body.require_main is False
    assert body.max_count == 100


@pytest.mark.asyncio
async def test_end_of_stream():
    """
    During the Read phase, we yield the events to the subscription so that the
    user is unaware of the chicanery in the background.

    When we reach the end of the stream, we should send a subscribe message to
    start the volatile subscription.
    """

    convo = CatchupAllSubscription()
    output = TeeQueue()
    await convo.start(output)

    event_1_id = uuid.uuid4()
    event_2_id = uuid.uuid4()

    response = (
        ReadAllEventsResponseBuilder()
        .with_event(event_id=event_1_id, event_number=32)
        .with_event(event_id=event_2_id, event_number=33)
        .at_end_of_stream()
    ).build()

    await reply_to(convo, response, output)

    subscription = await convo.result

    event_1 = await anext(subscription.events)
    event_2 = await anext(subscription.events)

    assert event_1.id == event_1_id
    assert event_1.event_number == 32

    assert event_2.id == event_2_id
    assert event_2.event_number == 33


@pytest.mark.asyncio
async def test_paging():
    """
   During the read phase, we expect to page through multiple batches of
   events. In this scenario we have two batches, each of two events.
   """

    convo = CatchupAllSubscription()
    output = TeeQueue()
    await convo.start(output)
    await output.get()

    event_1_id = uuid.uuid4()
    event_2_id = uuid.uuid4()
    event_3_id = uuid.uuid4()
    event_4_id = uuid.uuid4()

    first_response = (
        ReadAllEventsResponseBuilder()
        .with_event(event_id=event_1_id, event_number=32)
        .with_event(event_id=event_2_id, event_number=33)
        .with_position(1, 1)
        .with_next_position(2, 2)
    ).build()

    second_response = (
        ReadAllEventsResponseBuilder()
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
    body = proto.ReadAllEvents()
    body.ParseFromString(reply.payload)
    assert body.commit_position == 2
    assert body.prepare_position == 2

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

    convo = CatchupAllSubscription()
    output = TeeQueue()
    await convo.start(output)
    await output.get()

    await reply_to(
        convo, (ReadAllEventsResponseBuilder().at_end_of_stream()).build(), output
    )

    reply = await output.get()
    print(str(reply))
    payload = proto.SubscribeToStream()
    payload.ParseFromString(reply.payload)

    assert reply.command == msg.TcpCommand.SubscribeToStream
    assert payload.event_stream_id == ""
    assert payload.resolve_link_tos is True


@pytest.mark.asyncio
async def test_should_perform_a_catchup_when_subscription_is_confirmed():

    """
   When we have read all the events in the stream, we should send a
   request to subscribe for new events.

   We should start reading catchup events from the `next_commit_position`
   returned by the historical event read.
   """

    convo = CatchupAllSubscription()
    output = TeeQueue()
    await convo.start(output)

    await reply_to(
        convo,
        ReadAllEventsResponseBuilder()
        .with_position(18, 19)
        .with_next_position(18, 19)
        .build(),
        output,
    )
    await confirm_subscription(convo, output, event_number=42, commit_pos=40)
    [read_historial, subscribe, catch_up] = await output.next_event(3)

    assert read_historial.command == msg.TcpCommand.ReadAllEventsForward
    assert subscribe.command == msg.TcpCommand.SubscribeToStream
    assert catch_up.command == msg.TcpCommand.ReadAllEventsForward

    payload = proto.ReadAllEvents()
    payload.ParseFromString(catch_up.payload)

    assert payload.commit_position == 18
    assert payload.prepare_position == 19


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

    convo = CatchupAllSubscription()
    output = TeeQueue()
    await convo.start(output)
    await output.get()

    last_page = (
        ReadAllEventsResponseBuilder()
        .with_event(event_number=1, type="a")
        .at_end_of_stream()
        .build()
    )

    subscribed_event = event_appeared(event_number=4, type="d")

    first_catchup = (
        ReadAllEventsResponseBuilder().with_event(event_number=2, type="b").build()
    )
    second_catchup = (
        ReadAllEventsResponseBuilder()
        .with_event(event_number=3, type="c")
        .with_event(event_number=4, type="d")
    ).build()

    await reply_to(convo, last_page, output)
    assert (await output.get()).command == msg.TcpCommand.SubscribeToStream

    await confirm_subscription(convo, output, event_number=3)
    await reply_to(convo, subscribed_event, output)
    assert (await output.get()).command == msg.TcpCommand.ReadAllEventsForward

    await reply_to(convo, first_catchup, output)
    assert (await output.get()).command == msg.TcpCommand.ReadAllEventsForward

    await reply_to(convo, second_catchup, output)
    await drop_subscription(convo, output)

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


@pytest.mark.asyncio
async def test_subscription_dropped_mid_stream():
    convo = CatchupAllSubscription()
    output = TeeQueue()
    empty_page = ReadAllEventsResponseBuilder().at_end_of_stream().build()
    await reply_to(convo, empty_page, output)
    await confirm_subscription(convo, output, event_number=10, commit_pos=10)
    await reply_to(convo, empty_page, output)
    subscription = convo.result.result()

    await reply_to(convo, event_appeared(), output)
    await drop_subscription(convo, output)

    events = [e async for e in subscription.events]
    assert len(events) == 1


@pytest.mark.asyncio
async def test_subscription_failure_mid_stream():
    output = TeeQueue()
    convo = CatchupAllSubscription("my-stream")
    event_id = uuid.uuid4()

    await reply_to(convo, EMPTY_STREAM_PAGE, output)
    await confirm_subscription(convo, output, event_number=10, commit_pos=10)
    await reply_to(convo, EMPTY_STREAM_PAGE, output)
    subscription = convo.result.result()

    await reply_to(convo, event_appeared(event_id=event_id), output)
    await drop_subscription(
        convo, output, msg.SubscriptionDropReason.SubscriberMaxCountReached
    )

    with pytest.raises(exn.SubscriptionFailed):
        event = await anext(subscription.events)
        assert event.id == event_id

        await anext(subscription.events)


@pytest.mark.asyncio
async def test_unsubscription():
    correlation_id = uuid.uuid4()
    output = TeeQueue()
    convo = CatchupAllSubscription(conversation_id=correlation_id)
    await convo.start(output)

    await reply_to(convo, EMPTY_STREAM_PAGE, output)
    await confirm_subscription(convo, output, event_number=10, commit_pos=10)
    await reply_to(convo, EMPTY_STREAM_PAGE, output)

    sub = convo.result.result()
    await sub.unsubscribe()

    [read_historical, subscribe, catch_up, unsubscribe] = output.items

    assert unsubscribe.command == msg.TcpCommand.UnsubscribeFromStream
    assert unsubscribe.conversation_id == correlation_id


@pytest.mark.asyncio
async def test_subscribe_with_context_manager():
    conversation_id = uuid.uuid4()
    output = TeeQueue()
    convo = CatchupAllSubscription(conversation_id=conversation_id)
    await convo.start(output)

    # Create a subscription with three events in it
    await reply_to(convo, EMPTY_STREAM_PAGE, output)
    await confirm_subscription(convo, output, event_number=10, commit_pos=10)
    await reply_to(convo, EMPTY_STREAM_PAGE, output)

    for i in range(0, 3):
        await reply_to(
            convo, event_appeared(event_id=uuid.uuid4(), event_number=i), output
        )

    async with (await convo.result) as subscription:
        events_seen = 0
        async for _ in subscription.events:
            events_seen += 1

            if events_seen == 3:
                break

    # Having exited the context manager it should send
    # an unsubscribe message
    [read_historical, subscribe, catch_up, unsubscribe] = output.items

    assert unsubscribe.command == msg.TcpCommand.UnsubscribeFromStream
    assert unsubscribe.conversation_id == conversation_id


@pytest.mark.asyncio
async def test_restart_from_historical():
    """
   If we ask the conversation to start again while we're reading historical events
   we should re-send the most recent page request.

   In this scenario, we start reading the stream at event 10, we receive a
   page with 2 events, we request the next page starting at 12.

   When we restart the conversation, we should again request the page starting at 12.
   """

    conversation_id = uuid.uuid4()
    output = TeeQueue()
    convo = CatchupAllSubscription(
        start_from=msg.Position(10, 10), conversation_id=conversation_id
    )

    await convo.start(output)

    await reply_to(
        convo,
        (
            ReadAllEventsResponseBuilder()
            .with_event(event_number=10)
            .with_event(event_number=11)
            .with_next_position(12, 12)
            .build()
        ),
        output,
    )

    await convo.start(output)

    [first_page, second_page, second_page_again] = [
        read_as(proto.ReadStreamEvents, m) for m in output.items
    ]

    assert second_page.from_event_number == second_page_again.from_event_number


@pytest.mark.asyncio
async def test_restart_from_catchup():
    """
   If the connection drops during the catchup phase, we need to unsubscribe
   from the stream and then go back to reading historical events starting from
   the last page.

   => Request historical events
   <= Receive 1 event, next_event = 1
   => Subscribe
   <= Confirmed
   => Catch up from 1

   ** Restart **

   => Unsubscribe
   <= Confirmed
   => Read historical from 1
   <= Empty page
   => Subscribe
   """
    conversation_id = uuid.uuid4()
    output = TeeQueue()
    convo = CatchupAllSubscription(conversation_id=conversation_id)
    await convo.start(output)
    await output.get()

    page_one = (
        ReadAllEventsResponseBuilder()
        .with_event(event_number=1)
        .with_position(1, 1)
        .with_next_position(1, 1)
        .build()
    )

    await reply_to(convo, page_one, output)

    await output.get()
    await confirm_subscription(convo, output, event_number=10, commit_pos=10)

    first_catch_up = read_as(proto.ReadStreamEvents, await output.get())
    await reply_to(convo, page_one, output)

    # Restart
    await convo.start(output)
    unsubscribe = await output.get()

    assert first_catch_up.from_event_number == 1
    assert unsubscribe.command == msg.TcpCommand.UnsubscribeFromStream

    await drop_subscription(convo, output)
    second_catchup = read_as(proto.ReadStreamEvents, await output.get())

    assert second_catchup.from_event_number == 1


@pytest.mark.asyncio
async def test_historical_duplicates():
    """
   It's possible that we receive the reply to a ReadStreamEvents request after we've
   resent the request. This will result in our receiving a duplicate page.

   In this instance, we should not raise duplicate events.

   => Request historical
   RESTART
   => Request historical
   <= 2 events
   <= 3 events

   Should only see the 3 unique events
   """

    two_events = (
        ReadAllEventsResponseBuilder()
        .with_event(event_number=1)
        .with_event(event_number=2)
        .with_next_position(2, 2)
        .at_end_of_stream()
        .build()
    )

    three_events = (
        ReadAllEventsResponseBuilder()
        .with_event(event_number=1)
        .with_event(event_number=2)
        .with_event(event_number=3)
        .at_end_of_stream(3, 3)
        .build()
    )

    output = TeeQueue()
    convo = CatchupAllSubscription()
    await convo.start(output)
    await convo.start(output)

    await reply_to(convo, two_events, output)
    await reply_to(convo, three_events, output)

    [event_1, event_2, event_3] = await anext(convo.subscription.events, 3)

    print(event_1, event_2, event_3)

    assert event_1.event_number == 1
    assert event_2.event_number == 2
    assert event_3.event_number == 3


@pytest.mark.asyncio
async def test_subscription_duplicates():
    """
   If we receive subscription events while catching up, we buffer them internally.
   If we restart the conversation at that point we need to make sure we clear our buffer
   and do not raise duplicate events.

   => Request historical
   <= Empty
   => Subscribe to stream
   <= Confirmed
   => Request catchup
   <= Subscribed event 2 appeared
   <= Event 1, not end of stream

   RESTART

   => Drop subscription
   <= Dropped
   => Request historical from_event = 1
   <= Receive event 2 at end of stream
   => Subscribe
   <= Confirmed
   => Catchup
   <= Subscribed event 3 appeared
   <= Empty

   Should yield [event 1, event 2, event 3]
   """
    event_1_not_end_of_stream = (
        ReadAllEventsResponseBuilder()
        .with_event(event_number=1)
        .with_next_position(2, 2)
        .build()
    )

    event_2_at_end_of_stream = (
        ReadAllEventsResponseBuilder()
        .with_event(event_number=2)
        .at_end_of_stream(2, 2)
        .build()
    )

    output = TeeQueue()
    convo = CatchupAllSubscription()
    await convo.start(output)
    await reply_to(convo, EMPTY_STREAM_PAGE, output)
    await confirm_subscription(convo, output, event_number=10, commit_pos=10)
    await reply_to(convo, event_appeared(event_number=2), output)
    await reply_to(convo, event_1_not_end_of_stream, output)

    # RESTART
    await convo.start(output)
    output.items.clear()

    await drop_subscription(convo, output)

    second_read_historical = read_as(proto.ReadStreamEvents, output.items[0])

    await reply_to(convo, event_2_at_end_of_stream, output)
    await confirm_subscription(convo, output, event_number=10, commit_pos=10)
    await reply_to(convo, event_appeared(event_number=3), output)
    await reply_to(convo, EMPTY_STREAM_PAGE, output)

    [event_1, event_2, event_3] = await anext(convo.subscription.events, 3)
    assert event_1.event_number == 1
    assert event_2.event_number == 2
    assert event_3.event_number == 3

    assert second_read_historical.from_event_number == 2


@pytest.mark.asyncio
async def test_live_restart():
    """
   If we reset the conversation while we are live, we should first unsubscribe
   then start a historical read from the last read event.

   => Read historial
   <= empty
   => subscribe
   <= confirmed
   => catchup
   <= empty
   <= event 1 appeared
   <= event 2 appeared

   RESTART

   => unsubscribe
   <= dropped
   => Read historical from 2
   """

    output = TeeQueue()
    convo = CatchupAllSubscription()
    await convo.start(output)
    await reply_to(convo, EMPTY_STREAM_PAGE, output)
    await confirm_subscription(convo, output, event_number=10, commit_pos=10)
    await reply_to(convo, EMPTY_STREAM_PAGE, output)

    await reply_to(convo, event_appeared(event_number=11), output)
    await reply_to(convo, event_appeared(event_number=12), output)

    output.items.clear()

    await convo.start(output)
    await drop_subscription(convo, output)

    [unsubscribe, read_historical] = output.items
    read_historical = read_as(proto.ReadAllEvents, read_historical)

    assert unsubscribe.command == msg.TcpCommand.UnsubscribeFromStream
    assert read_historical.commit_position == 12
