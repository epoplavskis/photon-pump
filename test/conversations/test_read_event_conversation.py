from uuid import uuid4

from photonpump import messages as msg, exceptions
from photonpump import messages_pb2 as proto


def test_read_single_event():

    convo = msg.ReadEventConversation('my-stream', 23)
    request = convo.start()

    body = proto.ReadEvent()
    body.ParseFromString(request.payload)

    assert request.command is msg.TcpCommand.Read
    assert body.event_stream_id == 'my-stream'
    assert body.event_number == 23
    assert body.resolve_link_tos is True
    assert body.require_master is False


def test_read_single_event_success():

    event_id = uuid4()

    convo = msg.ReadEventConversation('my-stream', 23)
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

    reply = convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            response.SerializeToString()
        )
    )

    assert reply.action == msg.ReplyAction.CompleteScalar
    assert isinstance(reply.result, msg.Event)
    assert reply.result.event.stream == 'stream-123'
    assert reply.result.event.id == event_id
    assert reply.result.event.type == 'event-type'
    assert reply.result.event.event_number == 32

    assert reply.result.link is None


def error_result(error_code):
    data = bytearray(b'\x08\x00\x12\x00')
    data[1] = error_code
    return data


def test_event_not_found():

    convo = msg.ReadEventConversation('my-stream', 23)
    reply = convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            error_result(msg.ReadEventResult.NotFound)
        )
    )

    assert reply.action == msg.ReplyAction.CompleteError
    exn = reply.result
    assert isinstance(exn, exceptions.EventNotFound)
    assert exn.stream == 'my-stream'
    assert exn.event_number == 23


def test_stream_not_found():

    convo = msg.ReadEventConversation('my-stream', 23)
    reply = convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            error_result(msg.ReadEventResult.NoStream)
        )
    )

    assert reply.action == msg.ReplyAction.CompleteError
    exn = reply.result

    assert isinstance(exn, exceptions.StreamNotFound)
    assert exn.stream == 'my-stream'


def test_stream_deleted():
    convo = msg.ReadEventConversation('my-stream', 23)
    reply = convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            error_result(msg.ReadEventResult.StreamDeleted)
        )
    )

    assert reply.action == msg.ReplyAction.CompleteError
    exn = reply.result

    assert isinstance(exn, exceptions.StreamDeleted)
    assert exn.stream == 'my-stream'


def test_read_error():
    convo = msg.ReadEventConversation('my-stream', 23)
    reply =convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            error_result(msg.ReadEventResult.Error)
        )
    )

    assert reply.action == msg.ReplyAction.CompleteError
    exn = reply.result

    assert isinstance(exn, exceptions.ReadError)
    assert exn.stream == 'my-stream'


def test_access_denied():
    convo = msg.ReadEventConversation('my-stream', 23)
    reply = convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            error_result(msg.ReadEventResult.AccessDenied)
        )
    )

    assert reply.action == msg.ReplyAction.CompleteError
    exn = reply.result

    assert isinstance(exn, exceptions.AccessDenied)
    assert exn.conversation_type == 'ReadEventConversation'

