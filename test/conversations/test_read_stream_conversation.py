from asyncio import Queue
from uuid import uuid4

import pytest

from photonpump import messages as msg, exceptions
from photonpump import messages_pb2 as proto
from photonpump.conversations import ReplyAction, ReadStreamEvents


@pytest.mark.asyncio
async def test_read_stream_request():

    output = Queue()

    convo = ReadStreamEvents('my-stream', 23)
    await convo.start(output)
    request = await output.get()

    body = proto.ReadStreamEvents()
    body.ParseFromString(request.payload)

    assert request.command is msg.TcpCommand.ReadStreamEventsForward
    assert body.event_stream_id == 'my-stream'
    assert body.from_event_number == 23
    assert body.resolve_link_tos is True
    assert body.require_master is False
    assert body.max_count == 100


@pytest.mark.asyncio
async def test_read_stream_backward():

    output = Queue()
    convo = ReadStreamEvents('my-stream', 50, direction=msg.StreamDirection.Backward, max_count=10)
    await convo.start(output)
    request = await output.get()

    body = proto.ReadStreamEvents()
    body.ParseFromString(request.payload)

    assert request.command is msg.TcpCommand.ReadStreamEventsBackward
    assert body.event_stream_id == 'my-stream'
    assert body.from_event_number == 50
    assert body.resolve_link_tos is True
    assert body.require_master is False
    assert body.max_count == 10


@pytest.mark.asyncio
async def test_read_stream_success():

    event_1_id = uuid4()
    event_2_id = uuid4()

    convo = ReadStreamEvents('my-stream', 0)
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
    event_1.event.event_type = 'event-type'
    event_1.event.data_content_type = msg.ContentType.Json
    event_1.event.metadata_content_type = msg.ContentType.Binary
    event_1.event.data = """
    {
        'color': 'red',
        'winner': true
    }
    """.encode('UTF-8')

    event_2 = proto.ResolvedIndexedEvent()
    event_2.CopyFrom(event_1)
    event_2.event.event_type = 'event-2-type'
    event_2.event.event_id = event_2_id.bytes_le
    event_2.event.event_number = 33

    response.events.extend([event_1, event_2])

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            response.SerializeToString()
        ), None
    )

    result = await convo.result

    assert isinstance(result, msg.StreamSlice)

    [event_1, event_2] = result.events
    assert event_1.event.stream == 'stream-123'
    assert event_1.event.id == event_1_id
    assert event_1.event.type == 'event-type'
    assert event_1.event.event_number == 32

    assert event_2.event.stream == 'stream-123'
    assert event_2.event.id == event_2_id
    assert event_2.event.type == 'event-2-type'
    assert event_2.event.event_number == 33


@pytest.mark.asyncio
async def test_stream_not_found():

    convo = ReadStreamEvents('my-stream')
    response = proto.ReadStreamEventsCompleted()
    response.result = msg.ReadStreamResult.NoStream
    response.is_end_of_stream = False
    response.next_event_number = 0
    response.last_event_number = 0
    response.last_commit_position = 0
    response.error = "Couldn't find me face"

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadStreamEventsForwardCompleted,
            response.SerializeToString()
        ), None
    )

    with pytest.raises(exceptions.StreamNotFound) as exn:
        await convo.result
        assert exn.stream == 'my-stream'
        assert exn.conversation_id == convo.conversation_id


@pytest.mark.asyncio
async def test_stream_deleted():

    convo = ReadStreamEvents('my-stream')
    response = proto.ReadStreamEventsCompleted()
    response.result = msg.ReadStreamResult.StreamDeleted
    response.is_end_of_stream = False
    response.next_event_number = 0
    response.last_event_number = 0
    response.last_commit_position = 0

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadStreamEventsForwardCompleted,
            response.SerializeToString()
        ), None
    )

    with pytest.raises(exceptions.StreamDeleted) as exn:
        await convo.result
        assert exn.stream == 'my-stream'
        assert exn.conversation_id == convo.conversation_id


@pytest.mark.asyncio
async def test_read_error():

    convo = ReadStreamEvents('my-stream')
    response = proto.ReadStreamEventsCompleted()
    response.result = msg.ReadStreamResult.Error
    response.is_end_of_stream = False
    response.next_event_number = 0
    response.last_event_number = 0
    response.last_commit_position = 0
    response.error = "Something really weird just happened"

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadStreamEventsForwardCompleted,
            response.SerializeToString()
        ), None
    )

    with pytest.raises(exceptions.ReadError) as exn:
        await convo.result
        assert exn.stream == 'my-stream'
        assert exn.conversation_id == convo.conversation_id


@pytest.mark.asynio
async def test_access_denied():

    convo = ReadStreamEvents('my-stream')
    response = proto.ReadStreamEventsCompleted()
    response.result = msg.ReadStreamResult.AccessDenied
    response.is_end_of_stream = False
    response.next_event_number = 0
    response.last_event_number = 0
    response.last_commit_position = 0

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadStreamEventsForwardCompleted,
            response.SerializeToString()
        ), None
    )

    with pytest.raises(exceptions.AccessDenied) as exn:
        await convo.result

        assert exn.conversation_id == convo.conversation_id
        assert exn.conversation_type == 'ReadStreamEvents'
