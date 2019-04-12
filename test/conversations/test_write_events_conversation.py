from asyncio import Queue
import json
from uuid import uuid4

import pytest

import photonpump.messages as msg
import photonpump.messages_pb2 as proto
from photonpump.conversations import WriteEvents


def given_a_write_events_message():

    event_id = uuid4()
    conversation_id = uuid4()

    event_type = "pony_jumped"
    data = {"pony_name": "Burning Sulphur", "distance": 6}
    event_data = msg.NewEventData(event_id, event_type, data, None)

    conversation = WriteEvents(
        "my-stream", [event_data], conversation_id=conversation_id
    )

    return conversation


@pytest.mark.asyncio
async def test_write_one_event():

    output = Queue()

    event_id = uuid4()
    conversation_id = uuid4()

    event_type = "pony_jumped"
    data = {"pony_name": "Burning Sulphur", "distance": 6}
    event_data = msg.NewEventData(event_id, event_type, data, None)

    conversation = WriteEvents(
        "my-stream", [event_data], conversation_id=conversation_id
    )

    await conversation.start(output)
    request = await output.get()
    assert request.conversation_id == conversation_id
    assert request.command == msg.TcpCommand.WriteEvents

    assert not conversation.is_complete
    assert not conversation.result.done()

    _validate_write_request(
        request,
        "my-stream",
        event_type,
        event_id,
        msg.ContentType.Json,
        json.dumps(data).encode("UTF-8"),
    )


@pytest.mark.asyncio
async def test_write_one_event_binary():

    output = Queue()

    event_id = uuid4()
    conversation_id = uuid4()

    event_type = "pony_jumped_binary"
    data = b"my binary encoded data"
    event_data = msg.NewEventData(event_id, event_type, data, None)

    conversation = WriteEvents(
        "my-stream", [event_data], conversation_id=conversation_id
    )

    await conversation.start(output)
    request = await output.get()
    assert request.conversation_id == conversation_id
    assert request.command == msg.TcpCommand.WriteEvents

    assert not conversation.is_complete
    assert not conversation.result.done()

    _validate_write_request(
        request, "my-stream", event_type, event_id, msg.ContentType.Binary, data
    )


def _validate_write_request(
    request, stream_id, event_type, event_id, content_type, data
):

    payload = proto.WriteEvents()
    payload.ParseFromString(request.payload)

    assert payload.event_stream_id == stream_id
    assert payload.expected_version == msg.ExpectedVersion.Any
    assert len(payload.events) == 1
    assert not payload.require_master

    [evt] = payload.events
    assert evt.event_id == event_id.bytes_le
    assert evt.event_type == event_type

    assert evt.data_content_type == content_type
    assert evt.data == data

    assert evt.metadata_content_type == msg.ContentType.Binary
    assert evt.metadata == b""


@pytest.mark.asyncio
async def test_one_event_response():

    output = Queue()
    conversation = given_a_write_events_message()

    await conversation.start(output)

    await output.get()
    payload = proto.WriteEventsCompleted()
    payload.result = msg.OperationResult.Success
    payload.first_event_number = 73
    payload.last_event_number = 73

    await conversation.respond_to(
        msg.InboundMessage(
            conversation.conversation_id,
            msg.TcpCommand.WriteEventsCompleted,
            payload.SerializeToString(),
        ),
        output,
    )

    result = await conversation.result

    assert result.first_event_number == 73
    assert result.last_event_number == 73
    assert result.result == msg.OperationResult.Success


@pytest.mark.skip(reason="upcoming feature")
@pytest.mark.asyncio
async def test_timeout():
    """
    If we time out while waiting for a write events result
    then we should resubmit the original message.
    """

    output = Queue()
    conversation = given_a_write_events_message()
    await conversation.start(output)

    await conversation.timeout()


@pytest.mark.skip(reason="upcoming feature")
@pytest.mark.asyncio
async def test_connection_failed():
    """
    IF the connection shuts down while we're waiting for a response
    then all we can do is cancel the result.
    """

    output = Queue()
    conversation = given_a_write_events_message()
    await conversation.start(output)

    await conversation.stop()
