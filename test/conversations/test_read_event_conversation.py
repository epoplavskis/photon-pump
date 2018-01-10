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

    assert not convo.is_complete
    assert not convo.result.done()


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

    convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            response.SerializeToString()
        )
    )

    assert convo.is_complete
    result = convo.result.result()

    assert isinstance(result, msg.Event)
    assert result.event.stream == 'stream-123'
    assert result.event.id == event_id
    assert result.event.type == 'event-type'
    assert result.event.event_number == 32

    assert result.link is None


def test_event_not_found():

    convo = msg.ReadEventConversation('my-stream', 23)
    convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            b'\x08\x01\x12\x00'
        )
    )

    assert convo.is_complete
    exn = convo.result.exception()

    assert isinstance(exn, exceptions.EventNotFound)
    assert exn.stream == 'my-stream'
    assert exn.event_number == 23


def test_stream_not_found():

    convo = msg.ReadEventConversation('my-stream', 23)
    convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            b'\x08\x02\x12\x00'
        )
    )

    assert convo.is_complete
    exn = convo.result.exception()

    assert isinstance(exn, exceptions.StreamNotFound)
    assert exn.stream == 'my-stream'


def test_stream_deleted():
    convo = msg.ReadEventConversation('my-stream', 23)
    convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            b'\x08\x03\x12\x00'
        )
    )

    assert convo.is_complete
    exn = convo.result.exception()

    assert isinstance(exn, exceptions.StreamDeleted)
    assert exn.stream == 'my-stream'


def test_read_error():
    convo = msg.ReadEventConversation('my-stream', 23)
    convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            b'\x08\x04\x12\x00'
        )
    )

    assert convo.is_complete
    exn = convo.result.exception()

    assert isinstance(exn, exceptions.ReadError)
    assert exn.stream == 'my-stream'


def test_access_denied():
    convo = msg.ReadEventConversation('my-stream', 23)
    convo.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.ReadEventCompleted,
            b'\x08\x05\x12\x00'
        )
    )

    assert convo.is_complete
    exn = convo.result.exception()

    assert isinstance(exn, exceptions.AccessDenied)
    assert exn.conversation_type == 'ReadEventConversation'

