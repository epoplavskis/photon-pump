from uuid import uuid4

from photonpump import messages as msg
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
