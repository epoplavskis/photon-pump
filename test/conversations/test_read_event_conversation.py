from asyncio import Queue
from uuid import uuid4

import pytest

from photonpump import exceptions
from photonpump import messages as msg
from photonpump import messages_pb2 as proto
from photonpump.conversations import ReadEvent


@pytest.mark.asyncio
async def test_read_single_event():

    output = Queue()
    convo = ReadEvent('my-stream', 23)
    await convo.start(output)

    request = await output.get()

    body = proto.ReadEvent()
    body.ParseFromString(request.payload)

    assert request.command is msg.TcpCommand.Read
    assert body.event_stream_id == 'my-stream'
    assert body.event_number == 23
    assert body.resolve_link_tos is True
    assert body.require_master is False


@pytest.mark.asyncio
async def test_read_single_event_success():

    event_id = uuid4()

    convo = ReadEvent('my-stream', 23)
    response = proto.ReadEventCompleted()
    response.result = msg.ReadEventResult.Success

    response.event.event.event_stream_id = "stream-123"
    response.event.event.event_number = 32
    response.event.event.event_id = event_id.bytes_le
    response.event.event.event_type = 'event-type'
    response.event.event.data_content_type = msg.ContentType.Json
    response.event.event.metadata_content_type = msg.ContentType.Binary
    response.event.event.data = """
    {
        'color': 'red',
        'winner': true
    }
    """.encode('UTF-8')

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            response.SerializeToString()
        ), None
    )

    result = await convo.result

    assert result.event.stream == 'stream-123'
    assert result.event.id == event_id
    assert result.event.type == 'event-type'
    assert result.event.event_number == 32

    assert result.link is None


def error_result(error_code):
    data = bytearray(b'\x08\x00\x12\x00')
    data[1] = error_code

    return data


@pytest.mark.asyncio
async def test_event_not_found():

    convo = ReadEvent('my-stream', 23)
    await convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            error_result(msg.ReadEventResult.NotFound)
        ), None
    )

    with pytest.raises(exceptions.EventNotFound) as exn:
        await convo.result
        assert exn.stream == 'my-stream'
        assert exn.event_number == 23


@pytest.mark.asyncio
async def test_stream_not_found():

    convo = ReadEvent('my-stream', 23)
    await convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            error_result(msg.ReadEventResult.NoStream)
        ), None
    )

    with pytest.raises(exceptions.StreamNotFound) as exn:
        await convo.result
        assert exn.stream == 'my-stream'


@pytest.mark.asyncio
async def test_stream_deleted():
    convo = ReadEvent('my-stream', 23)
    await convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            error_result(msg.ReadEventResult.StreamDeleted)
        ), None
    )

    with pytest.raises(exceptions.StreamDeleted) as exn:
        await convo.result
        assert exn.stream == 'my-stream'


@pytest.mark.asyncio
async def test_read_error():
    convo = ReadEvent('my-stream', 23)
    await convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            error_result(msg.ReadEventResult.Error)
        ), None
    )

    with pytest.raises(exceptions.ReadError) as exn:
        await convo.result
        assert exn.stream == 'my-stream'


@pytest.mark.asyncio
async def test_access_denied():
    convo = ReadEvent('my-stream', 23)
    await convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            error_result(msg.ReadEventResult.AccessDenied)
        ), None
    )

    with pytest.raises(exceptions.AccessDenied) as exn:
        await convo.result
        assert exn.conversation_type == 'ReadEvent'
