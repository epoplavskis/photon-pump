import json
from uuid import uuid4

import photonpump.messages as msg
import photonpump.messages_pb2 as proto
from photonpump.conversations import ReplyAction, WriteEvents


def given_a_write_events_message():

    event_id = uuid4()
    conversation_id = uuid4()

    event_type = "pony_jumped"
    data = {'pony_name': 'Burning Sulphur', 'distance': 6}
    event_data = msg.NewEventData(event_id, event_type, data, None)

    conversation = WriteEvents(
        "my-stream", [event_data], conversation_id=conversation_id
    )

    return conversation


def test_write_one_event():

    event_id = uuid4()
    conversation_id = uuid4()

    event_type = "pony_jumped"
    data = {'pony_name': 'Burning Sulphur', 'distance': 6}
    event_data = msg.NewEventData(event_id, event_type, data, None)

    conversation = WriteEvents(
        "my-stream", [event_data], conversation_id=conversation_id
    )

    request = conversation.start()
    assert request.conversation_id == conversation_id
    assert request.command == msg.TcpCommand.WriteEvents

    assert not conversation.is_complete
    assert not conversation.result.done()

    payload = proto.WriteEvents()
    payload.ParseFromString(request.payload)

    assert payload.event_stream_id == 'my-stream'
    assert payload.expected_version == msg.ExpectedVersion.Any
    assert len(payload.events) == 1
    assert not payload.require_master

    [evt] = payload.events
    assert evt.event_id == event_id.bytes_le
    assert evt.event_type == 'pony_jumped'

    assert evt.data_content_type == msg.ContentType.Json
    assert evt.data == json.dumps(data).encode('UTF-8')

    assert evt.metadata_content_type == msg.ContentType.Binary
    assert evt.metadata == b''


def test_one_event_response():

    conversation = given_a_write_events_message()

    conversation.start()

    payload = proto.WriteEventsCompleted()
    payload.result = msg.OperationResult.Success
    payload.first_event_number = 73
    payload.last_event_number = 73

    reply = conversation.respond_to(
        msg.InboundMessage(
            conversation.conversation_id, msg.TcpCommand.WriteEventsCompleted,
            payload.SerializeToString()
        )
    )

    assert reply.next_message is None
    result = reply.result

    assert result.first_event_number == 73
    assert result.last_event_number == 73
    assert result.result == msg.OperationResult.Success


def test_timeout():
    """
    If we time out while waiting for a write events result
    then we should resubmit the original message.
    """

    conversation = given_a_write_events_message()
    conversation.start()

    reply = conversation.timeout()

    assert reply.next_message == conversation.start()
    assert reply.action == ReplyAction.ResubmitMessage


def test_connection_failed():
    """
    IF the connection shuts down while we're waiting for a response
    then all we can do is cancel the result.
    """

    conversation = given_a_write_events_message()
    conversation.start()

    reply = conversation.stop()

    assert reply.action == ReplyAction.CancelFuture
    assert reply.next_message is None
    assert reply.result is None
