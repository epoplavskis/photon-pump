import asyncio
import logging
import uuid

import pytest
from testfixtures import LogCapture

from photonpump.connection import MessageDispatcher
from photonpump.conversations import Conversation, Heartbeat, Ping, ReplyAction
from photonpump.messages import InboundMessage, OutboundMessage, TcpCommand


def start_dispatcher():
    input = asyncio.Queue()
    output = asyncio.Queue()
    dispatcher = MessageDispatcher(input=input, output=output)
    dispatcher.start()

    return input, output, dispatcher


@pytest.mark.asyncio
async def test_when_enqueuing_a_conversation():
    """
    When we enqueue a conversation, we should add the first message
    of the conversation to the outbound queue.
    """

    conversation = Ping()

    _, out_queue, dispatcher = start_dispatcher()

    await dispatcher.enqueue_conversation(conversation)

    msg = out_queue.get_nowait()

    assert msg == conversation.start()
    assert dispatcher.has_conversation(conversation.conversation_id)


@pytest.mark.asyncio
async def test_when_receiving_a_response_to_ping():
    """
    When ping responds, we should set a result and remove the
    conversation from the active_conversations list.
    """
    conversation = Ping()

    in_queue, _, dispatcher = start_dispatcher()

    future = await dispatcher.enqueue_conversation(conversation)
    await in_queue.put(
        InboundMessage(conversation.conversation_id, TcpCommand.Pong, bytes())
    )
    await asyncio.wait_for(future, 1)
    assert not dispatcher.has_conversation(conversation.conversation_id)


@pytest.mark.asyncio
async def test_when_receiving_a_heartbeat_request():
    """
    When we receive a HeartbeatRequest from the server, we should
    immediately respond with a HeartbeatResponse
    """
    in_queue, out_queue, dispatcher = start_dispatcher()

    heartbeat_id = uuid.uuid4()

    await in_queue.put(
        InboundMessage(heartbeat_id, TcpCommand.HeartbeatRequest, bytes())
    )

    response = await out_queue.get()
    assert response == OutboundMessage(
        heartbeat_id, TcpCommand.HeartbeatResponse, bytes()
    )


@pytest.mark.asyncio
async def test_when_no_message_is_received():
    """
    If, for whatever reason, we get None on the input queue
    we should just ignore it and carry on regardless.
    """

    conversation = Ping()
    in_queue, out_queue, dispatcher = start_dispatcher()
    future = await dispatcher.enqueue_conversation(conversation)

    # We're going to throw this None in there and make sure it doesn't break
    await in_queue.put(None)
    await in_queue.put(
        InboundMessage(conversation.conversation_id, TcpCommand.Pong, bytes())
    )

    await asyncio.wait_for(future, 1)
    assert future.result()


@pytest.mark.asyncio
async def test_when_the_conversation_is_not_recognised():
    unexpected_conversation_id = uuid.uuid4()
    unexpected_message = InboundMessage(
        unexpected_conversation_id, TcpCommand.Pong, bytes()
    )
    conversation = Ping()

    in_queue, _, dispatcher = start_dispatcher()
    future = await dispatcher.enqueue_conversation(conversation)
    with LogCapture() as l:
        await in_queue.put(unexpected_message)
        await in_queue.put(
            InboundMessage(
                conversation.conversation_id, TcpCommand.Pong, bytes()
            )
        )
        assert await future
        l.check_present(
            (
                "photonpump.connection.MessageDispatcher", "ERROR",
                f"No conversation found for message {unexpected_message}"
            )
        )
