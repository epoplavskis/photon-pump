from asyncio import Queue
from uuid import uuid4

import pytest

from photonpump import messages as msg, exceptions
from photonpump import messages_pb2 as proto
from photonpump.conversations import ReadAllEvents


@pytest.mark.asyncio
async def test_read_all_request():

    output = Queue()

    convo = ReadAllEvents(msg.Position(10, 11))
    await convo.start(output)
    request = await output.get()

    body = proto.ReadAllEvents()
    body.ParseFromString(request.payload)

    assert request.command is msg.TcpCommand.ReadAllEventsForward
    assert body.commit_position == 10
    assert body.prepare_position == 11
    assert body.resolve_link_tos is True
    assert body.require_master is False
    assert body.max_count == 100


@pytest.mark.asyncio
async def test_read_all_backward():

    output = Queue()
    convo = ReadAllEvents(
        from_position=msg.Position(10, 11),
        direction=msg.StreamDirection.Backward,
        max_count=20,
    )
    await convo.start(output)
    request = await output.get()

    body = proto.ReadAllEvents()
    body.ParseFromString(request.payload)

    assert request.command is msg.TcpCommand.ReadAllEventsBackward
    assert body.commit_position == 10
    assert body.prepare_position == 11
    assert body.resolve_link_tos is True
    assert body.require_master is False
    assert body.max_count == 20


@pytest.mark.asyncio
async def test_read_all_success():

    event_1_id = uuid4()
    event_2_id = uuid4()

    convo = ReadAllEvents()
    response = proto.ReadAllEventsCompleted()
    response.result = msg.ReadEventResult.Success
    response.next_commit_position = 10
    response.next_prepare_position = 10
    response.commit_position = 9
    response.prepare_position = 9

    event_1 = proto.ResolvedEvent()
    event_1.commit_position = 8
    event_1.prepare_position = 8
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
    event_2.event.event_stream_id = "stream-456"
    event_2.event.event_type = "event-2-type"
    event_2.event.event_id = event_2_id.bytes_le
    event_2.event.event_number = 32

    response.events.extend([event_1, event_2])

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(),
            msg.TcpCommand.ReadAllEventsForwardCompleted,
            response.SerializeToString(),
        ),
        None,
    )

    result = await convo.result

    assert isinstance(result, msg.AllStreamSlice)

    [event_1, event_2] = result.events
    assert event_1.stream == "stream-123"
    assert event_1.id == event_1_id
    assert event_1.type == "event-type"
    assert event_1.event_number == 32

    assert event_2.stream == "stream-456"
    assert event_2.id == event_2_id
    assert event_2.type == "event-2-type"
    assert event_2.event_number == 32


@pytest.mark.asyncio
async def test_all_events_error():

    convo = ReadAllEvents()
    response = proto.ReadAllEventsCompleted()
    response.result = msg.ReadAllResult.Error
    response.next_commit_position = 10
    response.next_prepare_position = 10
    response.commit_position = 9
    response.prepare_position = 9
    response.error = "Something really weird just happened"

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(),
            msg.TcpCommand.ReadAllEventsForwardCompleted,
            response.SerializeToString(),
        ),
        None,
    )

    with pytest.raises(exceptions.ReadError) as exn:
        await convo.result
        assert exn.stream == "$all"
        assert exn.conversation_id == convo.conversation_id


@pytest.mark.asyncio
async def test_all_events_access_denied():

    convo = ReadAllEvents()
    response = proto.ReadAllEventsCompleted()
    response.result = msg.ReadAllResult.AccessDenied
    response.next_commit_position = 10
    response.next_prepare_position = 10
    response.commit_position = 9
    response.prepare_position = 9

    await convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadAllEventsForward, response.SerializeToString()
        ),
        None,
    )

    with pytest.raises(exceptions.AccessDenied) as exn:
        await convo.result

        assert exn.conversation_id == convo.conversation_id
        assert exn.conversation_type == "ReadAllEvents"
