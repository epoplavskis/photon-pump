import asyncio
import logging
import uuid

import pytest
from testfixtures import LogCapture

from photonpump.connection import MessageDispatcher
from photonpump.conversations import IterStreamEvents, Ping
from photonpump.exceptions import NotAuthenticated, PayloadUnreadable
from photonpump.messages import (
    InboundMessage, NewEvent, OutboundMessage, TcpCommand
)
from ..data import read_stream_events_completed


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
    dispatcher.stop()


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
    dispatcher.stop()


@pytest.mark.asyncio
async def test_when_the_conversation_is_not_recognised():
    """
    If the server sends us a message for a conversation we're not
    tracking, all we an really do is log an error and carry on.
    """
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
    dispatcher.stop()


@pytest.mark.asyncio
async def test_when_the_conversation_raises_an_error():
    """
    If the conversation raises an error, that error should be returned
    to the caller.
    """
    in_queue, _, dispatcher = start_dispatcher()
    conversation = Ping()
    future = await dispatcher.enqueue_conversation(conversation)

    await in_queue.put(
        InboundMessage(
            conversation.conversation_id, TcpCommand.NotAuthenticated, bytes()
        )
    )

    with pytest.raises(NotAuthenticated):
        await asyncio.wait_for(future, 1)

    assert not dispatcher.has_conversation(conversation.conversation_id)
    dispatcher.stop()


@pytest.mark.asyncio
async def test_when_the_conversation_receives_an_unexpected_response():
    """
    If the conversation raises an error, that error should be returned
    to the caller.
    """
    in_queue, _, dispatcher = start_dispatcher()
    conversation = Ping()
    future = await dispatcher.enqueue_conversation(conversation)

    await in_queue.put(
        InboundMessage(
            conversation.conversation_id, TcpCommand.WriteEventsCompleted,
            bytes()
        )
    )

    with pytest.raises(PayloadUnreadable):
        await asyncio.wait_for(future, 1)

    assert not dispatcher.has_conversation(conversation.conversation_id)
    dispatcher.stop()


async def anext(it):
    async for elem in it:
        return elem


@pytest.mark.asyncio
async def test_when_dispatching_stream_iterators():
    """
    When we're dealing with iterators, the first message should result
    in a StreamingIterator being returned to the caller; subsequent messages
    should push new events to the caller, and the final message should
    terminate iteration.
    """

    in_queue, _, dispatcher = start_dispatcher()
    conversation = IterStreamEvents("my-stream")
    first_msg = read_stream_events_completed(
        conversation.conversation_id, "my-stream",
        [NewEvent("event", data={"x": 1})]
    )
    second_msg = read_stream_events_completed(
        conversation.conversation_id, "my-stream",
        [NewEvent("event", data={"x": 2}),
         NewEvent("event", data={"x": 3})]
    )
    final_msg = read_stream_events_completed(
        conversation.conversation_id,
        "my-stream", [NewEvent("event", data={"x": 4})],
        end_of_stream=True
    )

    future = await dispatcher.enqueue_conversation(conversation)

    # The first message should result in an iterator being returned to the caller
    await in_queue.put(first_msg)
    iterator = await asyncio.wait_for(future, 1)

    e = await anext(iterator)
    assert e.event.json()['x'] == 1

    # The second message should result in two events on the iterator
    await in_queue.put(second_msg)

    e = await anext(iterator)
    assert e.event.json()['x'] == 2

    e = await anext(iterator)
    assert e.event.json()['x'] == 3

    # The final message should result in one event and the iterator terminating
    await in_queue.put(final_msg)

    [e] = [e async for e in iterator]
    assert e.event.json()['x'] == 4

