import json
from uuid import uuid4

import photonpump.exceptions as exn
import photonpump.messages as msg
import photonpump.messages_pb2 as proto


def test_write_one_event():

    event_id = uuid4()
    conversation_id = uuid4()

    event_type = "pony_jumped"
    data = {'pony_name': 'Burning Sulphur', 'distance': 6}
    event_data = msg.NewEventData(event_id, event_type, data, None)

    conversation = msg.WriteEventsConversation(
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

    event_id = uuid4()
    conversation_id = uuid4()

    event_type = "pony_jumped"
    data = {'pony_name': 'Burning Sulphur', 'distance': 6}
    event_data = msg.NewEventData(event_id, event_type, data, None)

    conversation = msg.WriteEventsConversation(
        "my-stream", [event_data], conversation_id=conversation_id
    )

    conversation.start()

    payload = proto.WriteEventsCompleted()
    payload.result = msg.OperationResult.Success
    payload.first_event_number = 73
    payload.last_event_number = 73

    next_msg = conversation.on_response(
        msg.InboundMessage(
            conversation_id, msg.TcpCommand.WriteEventsCompleted,
            payload.SerializeToString()
        )
    )

    assert next_msg is None
    assert conversation.is_complete
    result = conversation.result.result()

    assert result.first_event_number == 73
    assert result.last_event_number == 73
    assert result.result == msg.OperationResult.Success


def test_bad_request():

    event_id = uuid4()
    conversation_id = uuid4()
    error_message = "That's not an acceptable message, man"

    event_type = "pony_jumped"
    data = {'pony_name': 'Burning Sulphur', 'distance': 6}
    event_data = msg.NewEventData(event_id, event_type, data, None)

    conversation = msg.WriteEventsConversation(
        "my-stream", [event_data], conversation_id=conversation_id
    )

    conversation.start()
    conversation.on_response(
        msg.InboundMessage(
            conversation_id, msg.TcpCommand.BadRequest,
            error_message.encode('UTF-8')
        )
    )

    assert conversation.is_complete
    assert isinstance(conversation.result.exception(), exn.BadRequest)
    assert(conversation.result.exception().message == error_message)

