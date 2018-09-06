import asyncio
import pytest
import uuid
from ..fakes import TeeQueue
from photonpump.conversations import CatchupSubscription
from photonpump import messages as msg
from photonpump import messages_pb2 as proto


async def anext(it):
    return await asyncio.wait_for(it.anext(), 1)


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

    response = proto.ReadStreamEventsCompleted()
    response.result = msg.ReadEventResult.Success
    response.next_event_number = 10
    response.last_event_number = 9
    response.is_end_of_stream = True
    response.last_commit_position = 8

    event_1 = proto.ResolvedIndexedEvent()
    event_1.event.event_stream_id = "stream-123"
    event_1.event.event_number = 32
    event_1.event.event_id = event_1_id.bytes_le
    event_1.event.event_type = "event-type"
    event_1.event.data_content_type = msg.ContentType.Json
    event_1.event.metadata_content_type = msg.ContentType.Binary
    event_1.event.data = """
    {
        'color': 'red',
        'winner': true
    }
    """.encode(
        "UTF-8"
    )

    event_2 = proto.ResolvedIndexedEvent()
    event_2.CopyFrom(event_1)
    event_2.event.event_type = "event-2-type"
    event_2.event.event_id = event_2_id.bytes_le
    event_2.event.event_number = 33

    response.events.extend([event_1, event_2])

    await convo.respond_to(
        msg.InboundMessage(
            uuid.uuid4(), msg.TcpCommand.ReadStreamEventsForwardCompleted, response.SerializeToString()
        ),
        output,
    )

    print(convo.result)
    subscription = await convo.result
    print("BORP")

    event_1 = await anext(subscription.events)
    event_2 = await anext(subscription.events)

    assert event_1.stream == "stream-123"
    assert event_1.id == event_1_id
    assert event_1.type == "event-type"
    assert event_1.event_number == 32

    assert event_2.stream == "stream-123"
    assert event_2.id == event_2_id
    assert event_2.type == "event-2-type"
    assert event_2.event_number == 33
