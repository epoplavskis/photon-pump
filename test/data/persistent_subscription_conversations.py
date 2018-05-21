import json
from uuid import UUID

from photonpump import messages as msg
from photonpump import messages_pb2 as proto


def persistent_subscription_confirmed(conversation_id: UUID, subscription_id: str):
    payload = proto.PersistentSubscriptionConfirmation()
    payload.last_commit_position = 1
    payload.last_event_number = 1
    payload.subscription_id = subscription_id

    return msg.OutboundMessage(
        conversation_id, msg.TcpCommand.PersistentSubscriptionConfirmation,
        payload.SerializeToString()
    )


def subscription_event_appeared(
        conversation_id: UUID,
        event: msg.NewEventData,
        stream: str="my-stream",
        event_number: int=1):
    payload = proto.PersistentSubscriptionStreamEventAppeared()
    e = payload.event.event
    e.event_stream_id = stream
    e.event_number = event_number
    e.event_id = event.id.bytes_le
    e.event_type = event.type
    e.data_content_type = msg.ContentType.Json
    e.metadata_content_type = msg.ContentType.Binary
    e.data = json.dumps(event.data).encode("UTF-8")

    return msg.InboundMessage(
            conversation_id,
            msg.TcpCommand.PersistentSubscriptionStreamEventAppeared,
            payload.SerializeToString())


def persistent_subscription_dropped(conversation_id: UUID, reason: msg.SubscriptionDropReason):
    payload = proto.SubscriptionDropped()
    payload.reason = reason

    return msg.OutboundMessage(
        conversation_id, msg.TcpCommand.SubscriptionDropped,
        payload.SerializeToString()
    )
