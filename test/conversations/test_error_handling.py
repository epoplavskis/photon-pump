from uuid import uuid4

import photonpump.exceptions as exn
import photonpump.messages as msg
import photonpump.messages_pb2 as proto


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
    conversation.respond_to(
        msg.InboundMessage(
            conversation_id, msg.TcpCommand.BadRequest,
            error_message.encode('UTF-8')
        )
    )

    assert conversation.is_complete
    assert isinstance(conversation.result.exception(), exn.BadRequest)
    assert (conversation.result.exception().message == error_message)


def test_not_authenticated():

    event_id = uuid4()
    conversation_id = uuid4()
    error_message = "Dude, like who even are you?"

    event_type = "pony_jumped"
    data = {'pony_name': 'Burning Sulphur', 'distance': 6}
    event_data = msg.NewEventData(event_id, event_type, data, None)

    conversation = msg.WriteEventsConversation(
        "my-stream", [event_data], conversation_id=conversation_id
    )

    conversation.start()
    conversation.respond_to(
        msg.InboundMessage(
            conversation_id, msg.TcpCommand.NotAuthenticated,
            error_message.encode('UTF-8')
        )
    )

    assert conversation.is_complete
    assert isinstance(conversation.result.exception(), exn.NotAuthenticated)
    assert (conversation.result.exception().message == error_message)


def test_notready_message():

    payload = proto.NotHandled()
    payload.reason = msg.NotHandledReason.NotReady
    conversation = msg.PingConversation()
    conversation.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.NotHandled, payload.SerializeToString()))

    assert conversation.is_complete
    assert conversation.result.done()

    error = conversation.result.exception()
    assert isinstance(error, exn.NotReady)
    assert error.conversation_id == conversation.conversation_id


def test_too_busy_message():

    payload = proto.NotHandled()
    payload.reason = msg.NotHandledReason.TooBusy
    conversation = msg.PingConversation()
    conversation.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.NotHandled, payload.SerializeToString()))

    assert conversation.is_complete
    assert conversation.result.done()

    error = conversation.result.exception()
    assert isinstance(error, exn.TooBusy)
    assert error.conversation_id == conversation.conversation_id


def test_not_master():

    payload = proto.NotHandled()
    payload.reason = msg.NotHandledReason.NotMaster
    conversation = msg.PingConversation()
    conversation.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.NotHandled, payload.SerializeToString()))

    assert conversation.is_complete
    assert conversation.result.done()

    error = conversation.result.exception()
    assert isinstance(error, exn.NotMaster)
    assert error.conversation_id == conversation.conversation_id


def test_decode_error():

    conversation = msg.PingConversation()
    conversation.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.NotHandled, b'\x08\2A'))

    assert conversation.is_complete
    assert conversation.result.done()

    error = conversation.result.exception()
    assert isinstance(error, exn.PayloadUnreadable)
    assert error.conversation_id == conversation.conversation_id
