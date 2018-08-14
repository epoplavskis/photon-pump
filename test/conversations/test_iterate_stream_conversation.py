from uuid import uuid4

from ..fakes import TeeQueue
import pytest

from photonpump import exceptions
from photonpump import messages as msg
from photonpump import messages_pb2 as proto
from photonpump.conversations import IterStreamEvents


@pytest.mark.asyncio
async def test_read_stream_request():

    output = TeeQueue()
    convo = IterStreamEvents("my-stream")
    await convo.start(output)
    request = await output.get()

    body = proto.ReadStreamEvents()
    body.ParseFromString(request.payload)

    assert request.command is msg.TcpCommand.ReadStreamEventsForward
    assert body.event_stream_id == "my-stream"
    assert body.from_event_number == 0
    assert body.resolve_link_tos is True
    assert body.require_master is False
    assert body.max_count == 100


@pytest.mark.asyncio
async def test_read_stream_backward():

    output = TeeQueue()
    convo = IterStreamEvents(
        "my-stream", direction=msg.StreamDirection.Backward, batch_size=10
    )
    await convo.start(output)
    request = await output.get()

    body = proto.ReadStreamEvents()
    body.ParseFromString(request.payload)

    assert request.command is msg.TcpCommand.ReadStreamEventsBackward
    assert body.event_stream_id == "my-stream"
    assert body.from_event_number == -1
    assert body.resolve_link_tos is True
    assert body.require_master is False
    assert body.max_count == 10


@pytest.mark.asyncio
async def test_end_of_stream():

    output = TeeQueue()
    event_1_id = uuid4()
    event_2_id = uuid4()

    convo = IterStreamEvents("my-stream")
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
            uuid4(), msg.TcpCommand.ReadEventCompleted, response.SerializeToString()
        ),
        output,
    )

    # Todo: Use a slice here so that we can give information
    # to the iterator about its position in the stream?
    # assert isinstance(reply.result, msg.StreamSlice)

    result = await convo.result
    [event_1, event_2] = [e async for e in result]
    assert event_1.event.stream == "stream-123"
    assert event_1.event.id == event_1_id
    assert event_1.event.type == "event-type"
    assert event_1.event.event_number == 32

    assert event_2.event.stream == "stream-123"
    assert event_2.event.id == event_2_id
    assert event_2.event.type == "event-2-type"
    assert event_2.event.event_number == 33


@pytest.mark.asyncio
async def test_stream_paging():

    output = TeeQueue()
    convo = IterStreamEvents("my-stream")
    response = proto.ReadStreamEventsCompleted()
    response.result = msg.ReadEventResult.Success
    response.next_event_number = 10
    response.last_event_number = 9
    response.is_end_of_stream = False
    response.last_commit_position = 8

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(),
            msg.TcpCommand.ReadStreamEventsForwardCompleted,
            response.SerializeToString(),
        ),
        output,
    )

    reply = await output.get()
    body = proto.ReadStreamEvents()
    body.ParseFromString(reply.payload)

    assert body.from_event_number == 10


@pytest.mark.asyncio
async def test_stream_not_found():

    output = TeeQueue()
    convo = IterStreamEvents("my-stream")
    response = proto.ReadStreamEventsCompleted()
    response.result = msg.ReadStreamResult.NoStream
    response.is_end_of_stream = False
    response.next_event_number = 0
    response.last_event_number = 0
    response.last_commit_position = 0

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(),
            msg.TcpCommand.ReadStreamEventsForwardCompleted,
            response.SerializeToString(),
        ),
        output,
    )

    with pytest.raises(exceptions.StreamNotFound) as exn:
        await convo.result
        assert exn.stream == "my-stream"
        assert exn.conversation_id == convo.conversation_id

    assert not output.items


@pytest.mark.asyncio
async def test_error_mid_stream():

    output = TeeQueue()
    convo = IterStreamEvents("my-stream")
    response = proto.ReadStreamEventsCompleted()
    response.result = msg.ReadStreamResult.Success
    response.is_end_of_stream = False
    response.next_event_number = 0
    response.last_event_number = 0
    response.last_commit_position = 0

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(),
            msg.TcpCommand.ReadStreamEventsForwardCompleted,
            response.SerializeToString(),
        ),
        output,
    )

    response.result = msg.ReadStreamResult.AccessDenied

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(),
            msg.TcpCommand.ReadStreamEventsForwardCompleted,
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
