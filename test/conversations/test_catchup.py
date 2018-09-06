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

        return response

    def to_string(self):
        return self.build().SerializeToString()


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
    ).to_string()

    await convo.respond_to(
        msg.InboundMessage(
            uuid.uuid4(), msg.TcpCommand.ReadStreamEventsForwardCompleted, response
        ),
        output,
    )

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
    ).to_string()

    second_response = (
        ReadStreamEventsResponseBuilder()
        .with_event(event_id=event_3_id, event_number=34)
        .with_event(event_id=event_4_id, event_number=35)
    ).to_string()

    await convo.respond_to(
        msg.InboundMessage(
            uuid.uuid4(),
            msg.TcpCommand.ReadStreamEventsForwardCompleted,
            first_response,
        ),
        output,
    )

    subscription = await convo.result

    event_1 = await anext(subscription.events)
    event_2 = await anext(subscription.events)
    assert event_1.id == event_1_id
    assert event_2.id == event_2_id

    reply = await output.get()
    body = proto.ReadStreamEvents()
    body.ParseFromString(reply.payload)
    assert body.from_event_number == 34

    await convo.respond_to(
        msg.InboundMessage(
            uuid.uuid4(),
            msg.TcpCommand.ReadStreamEventsForwardCompleted,
            second_response,
        ),
        output,
    )

    event_3 = await anext(subscription.events)
    event_4 = await anext(subscription.events)
    assert event_3.id == event_3_id
    assert event_4.id == event_4_id

