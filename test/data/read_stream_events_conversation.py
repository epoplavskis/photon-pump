import json
from photonpump import exceptions
from photonpump import messages as msg
from photonpump import messages_pb2 as proto
from photonpump.conversations import ReadStreamEvents, ReplyAction


def read_stream_events_completed(
        conversation_id, stream, events, end_of_stream=False
):
    payload = proto.ReadStreamEventsCompleted()
    payload.result = msg.ReadStreamResult.Success
    payload.is_end_of_stream = end_of_stream

    for (idx, event) in enumerate(events):
        payload.next_event_number = idx + 1
        payload.last_event_number = idx
        payload.last_commit_position = idx

        e = payload.events.add()
        e.event.event_stream_id = stream
        e.event.event_number = idx
        e.event.event_id = event.id.bytes_le
        e.event.event_type = event.type
        e.event.data_content_type = msg.ContentType.Json
        e.event.metadata_content_type = msg.ContentType.Binary
        e.event.data = json.dumps(event.data).encode("UTF-8")

    return msg.InboundMessage(
        conversation_id, msg.TcpCommand.ReadStreamEventsForwardCompleted,
        payload.SerializeToString()
    )
