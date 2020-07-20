from uuid import uuid4

from ..fakes import TeeQueue
import pytest

from photonpump import exceptions
from photonpump import messages as msg
from photonpump import messages_pb2 as proto
from photonpump.conversations import IterAllEvents


@pytest.mark.asyncio
async def test_read_request():

    output = TeeQueue()
    convo = IterAllEvents(msg.Position(0, 0))
    await convo.start(output)
    request = await output.get()

    body = proto.ReadAllEvents()
    body.ParseFromString(request.payload)

    assert request.command is msg.TcpCommand.ReadAllEventsForward
    assert body.commit_position == 0
    assert body.prepare_position == 0
    assert body.resolve_link_tos is True
    assert body.require_main is False
    assert body.max_count == 100


@pytest.mark.asyncio
async def test_read_backward():

    output = TeeQueue()
    convo = IterAllEvents(direction=msg.StreamDirection.Backward, batch_size=10)
    await convo.start(output)
    request = await output.get()

    body = proto.ReadAllEvents()
    body.ParseFromString(request.payload)

    assert request.command is msg.TcpCommand.ReadAllEventsBackward
    assert body.commit_position == 0
    assert body.resolve_link_tos is True
    assert body.require_main is False
    assert body.max_count == 10


@pytest.mark.asyncio
async def test_end_of_stream():

    output = TeeQueue()
    event_1_id = uuid4()
    event_2_id = uuid4()

    convo = IterAllEvents()
    response = proto.ReadAllEventsCompleted()
    response.result = msg.ReadAllResult.Success
    response.commit_position = 10
    response.prepare_position = 10
    response.next_commit_position = 10
    response.next_prepare_position = 10

    event_1 = proto.ResolvedEvent()
    event_1.commit_position = 9
    event_1.prepare_position = 9
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

    event_2 = proto.ResolvedEvent()
    event_2.CopyFrom(event_1)
    event_2.commit_position = 10
    event_2.prepare_position = 10
    event_2.event.event_type = "event-2-type"
    event_2.event.event_id = event_2_id.bytes_le
    event_2.event.event_number = 33

    response.events.extend([event_1, event_2])

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(),
            msg.TcpCommand.ReadAllEventsForwardCompleted,
            response.SerializeToString(),
        ),
        output,
    )

    # Todo: Use a slice here so that we can give information
    # to the iterator about its position in the stream?
    # assert isinstance(reply.result, msg.StreamSlice)

    result = await convo.result
    [event_1, event_2] = [e async for e in result]
    assert event_1.stream == "stream-123"
    assert event_1.id == event_1_id
    assert event_1.type == "event-type"
    assert event_1.event_number == 32

    assert event_2.stream == "stream-123"
    assert event_2.id == event_2_id
    assert event_2.type == "event-2-type"
    assert event_2.event_number == 33


@pytest.mark.asyncio
async def test_stream_paging():

    output = TeeQueue()
    convo = IterAllEvents()
    response = proto.ReadAllEventsCompleted()
    response.result = msg.ReadEventResult.Success
    response.commit_position = 10
    response.prepare_position = 10
    response.next_commit_position = 11
    response.next_prepare_position = 12

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(),
            msg.TcpCommand.ReadStreamEventsForwardCompleted,
            response.SerializeToString(),
        ),
        output,
    )

    reply = await output.get()
    body = proto.ReadAllEvents()
    body.ParseFromString(reply.payload)

    assert body.commit_position == 11
    assert body.prepare_position == 12


@pytest.mark.asyncio
async def test_error_mid_stream():

    output = TeeQueue()
    convo = IterAllEvents("my-stream")
    response = proto.ReadAllEventsCompleted()
    response.result = msg.ReadStreamResult.Success
    response.commit_position = 0
    response.prepare_position = 0
    response.next_commit_position = 10
    response.next_prepare_position = 10

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(),
            msg.TcpCommand.ReadAllEventsForwardCompleted,
            response.SerializeToString(),
        ),
        output,
    )

    response.result = msg.ReadAllResult.AccessDenied

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(),
            msg.TcpCommand.ReadAllEventsForwardCompleted,
            response.SerializeToString(),
        ),
        output,
    )

    iterator = await convo.result
    with pytest.raises(exceptions.AccessDenied) as exn:
        await iterator.anext()

        assert exn.conversation_id == convo.conversation_id
        assert exn.conversation_type == "IterStreamEvents"

    assert len(output.items) == 1
