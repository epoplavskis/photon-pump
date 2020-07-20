from uuid import uuid4

import pytest

import photonpump.exceptions as exn
import photonpump.messages as msg
import photonpump.messages_pb2 as proto
from photonpump.conversations import Ping, WriteEvents

from ..fakes import TeeQueue


@pytest.mark.asyncio
async def test_bad_request():

    output = TeeQueue()

    event_id = uuid4()
    conversation_id = uuid4()
    error_message = "That's not an acceptable message, man"

    event_type = "pony_jumped"
    data = {"pony_name": "Burning Sulphur", "distance": 6}
    event_data = msg.NewEventData(event_id, event_type, data, None)

    conversation = WriteEvents(
        "my-stream", [event_data], conversation_id=conversation_id
    )

    await conversation.start(output)
    await conversation.respond_to(
        msg.InboundMessage(
            conversation_id, msg.TcpCommand.BadRequest, error_message.encode("UTF-8")
        ),
        output,
    )

    with pytest.raises(exn.BadRequest) as exc:
        await conversation.result
        assert exc.message == error_message


@pytest.mark.asyncio
async def test_not_authenticated():

    output = TeeQueue()

    event_id = uuid4()
    conversation_id = uuid4()
    error_message = "Dude, like who even are you?"

    event_type = "pony_jumped"
    data = {"pony_name": "Burning Sulphur", "distance": 6}
    event_data = msg.NewEventData(event_id, event_type, data, None)

    conversation = WriteEvents(
        "my-stream", [event_data], conversation_id=conversation_id
    )

    await conversation.start(output)
    await conversation.respond_to(
        msg.InboundMessage(
            conversation_id,
            msg.TcpCommand.NotAuthenticated,
            error_message.encode("UTF-8"),
        ),
        output,
    )

    with pytest.raises(exn.NotAuthenticated) as exc:
        await conversation.result
        assert exc.message == error_message


@pytest.mark.asyncio
async def test_notready_message():

    output = TeeQueue()
    payload = proto.NotHandled()
    payload.reason = msg.NotHandledReason.NotReady
    conversation = Ping()

    with pytest.raises(exn.NotReady):
        await conversation.respond_to(
            msg.InboundMessage(
                uuid4(), msg.TcpCommand.NotHandled, payload.SerializeToString()
            ),
            output,
        )

        await conversation.result

    assert conversation.is_complete


@pytest.mark.asyncio
async def test_too_busy_message():

    output = TeeQueue()

    payload = proto.NotHandled()
    payload.reason = msg.NotHandledReason.TooBusy

    conversation = Ping()
    await conversation.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.NotHandled, payload.SerializeToString()
        ),
        output,
    )

    with pytest.raises(exn.TooBusy) as exc:
        await conversation.result
        assert exc.conversatio_id == conversation.conversation_id


@pytest.mark.asyncio
async def test_not_main():

    output = TeeQueue()
    payload = proto.NotHandled()
    payload.reason = msg.NotHandledReason.NotMain

    conversation = Ping()
    await conversation.respond_to(
        msg.InboundMessage(
            uuid4(), msg.TcpCommand.NotHandled, payload.SerializeToString()
        ),
        output,
    )

    with pytest.raises(exn.NotMain) as exc:
        await conversation.result
        assert exc.conversation_id == conversation.conversation_id


@pytest.mark.asyncio
async def test_decode_error():
    """
    If we give the conversation an invalid payload, it should
    raise PayloadUnreadable.
    """

    output = TeeQueue()
    conversation = Ping()
    await conversation.respond_to(
        msg.InboundMessage(uuid4(), msg.TcpCommand.NotHandled, b"\x08\2A"), output
    )

    with pytest.raises(exn.PayloadUnreadable) as exc:
        await conversation.result
        assert exc.conversation_id == conversation.conversation_id

    assert conversation.is_complete
