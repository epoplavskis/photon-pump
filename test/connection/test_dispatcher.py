"""
This module tests the MessageDispatcher.

He has a terrible name, but he's a cool guy. He looks after the active
conversations list. You can give him a conversation to manage with the
enqueue_conversation method.

He'll call start() on the conversation and put the resulting OutboundMessage
on his output queue.

He'll listen for replies as InboundMessages on his input queue and match them
up with the conversation_id. He then calls ReplyTo on the conversation, passing
the InboundMessage. This results in a ReplyAction.

Right now, it's his job to make sure that the ReplyActions are handled properly.
This feels totally wrong but it means that the Conversation interface is super
simple and testable.
"""
import asyncio
import uuid

import pytest
from photonpump import messages_pb2 as proto
from photonpump.connection import MessageDispatcher
from photonpump.conversations import (ConnectPersistentSubscription,
                                      IterStreamEvents, Ping)
from photonpump.exceptions import (NotAuthenticated, PayloadUnreadable,
                                   StreamDeleted, SubscriptionCreationFailed,
                                   SubscriptionFailed)
from photonpump.messages import (InboundMessage, NewEvent, OutboundMessage,
                                 ReadStreamResult, SubscriptionDropReason,
                                 TcpCommand)
from testfixtures import LogCapture

from ..data import (persistent_subscription_confirmed,
                    persistent_subscription_dropped,
                    read_stream_events_completed, read_stream_events_failure,
                    subscription_event_appeared)


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
    with LogCapture() as logs:
        await in_queue.put(unexpected_message)
        await in_queue.put(
            InboundMessage(
                conversation.conversation_id, TcpCommand.Pong, bytes()
            )
        )
        assert await future
        logs.check_present(
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
    If the conversation receives an unhandled response, an error should
    be returned to the caller. This is a separate code path for now, but
    should probably be cleaned up in the Conversation. Maybe use a decorator?
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
    return await asyncio.wait_for(it.anext(), 1)


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
    # with one event. The conversation should still be active.
    await in_queue.put(first_msg)
    iterator = await asyncio.wait_for(future, 1)

    e = await anext(iterator)
    assert e.event.json()['x'] == 1
    assert dispatcher.has_conversation(conversation.conversation_id)

    # The second message should result in two events on the iterator.
    # The conversation should still be active.
    await in_queue.put(second_msg)

    e = await anext(iterator)
    assert e.event.json()['x'] == 2

    e = await anext(iterator)
    assert e.event.json()['x'] == 3
    assert dispatcher.has_conversation(conversation.conversation_id)

    # The final message should result in one event and the iterator terminating
    await in_queue.put(final_msg)

    [e] = [e async for e in iterator]
    assert e.event.json()['x'] == 4
    assert not dispatcher.has_conversation(conversation.conversation_id)


@pytest.mark.asyncio
async def test_when_a_stream_iterator_fails_midway():
    """
    Sometimes bad things happen. What happens if we're iterating a stream and
    some anarchist decides to delete it from under our feet? Well, I guess we
    should return an error to the caller and stop the iterator.
    """
    in_queue, _, dispatcher = start_dispatcher()
    conversation = IterStreamEvents("my-stream")
    first_msg = read_stream_events_completed(
        conversation.conversation_id, "my-stream",
        [NewEvent("event", data={"x": 1})]
    )
    second_msg = read_stream_events_failure(
        conversation.conversation_id, ReadStreamResult.StreamDeleted
    )

    future = await dispatcher.enqueue_conversation(conversation)

    await in_queue.put(first_msg)
    await in_queue.put(second_msg)

    iterator = await asyncio.wait_for(future, 1)

    event = await anext(iterator)
    assert event.event.json()['x'] == 1

    with pytest.raises(StreamDeleted):
        await anext(iterator)

    assert not dispatcher.has_conversation(conversation.conversation_id)
    dispatcher.stop()


@pytest.mark.asyncio
async def test_when_a_stream_iterator_fails_at_the_first_hurdle():
    """
    If we receive an error in reply to the first message in an iterator
    conversation, then we ought to raise the error instead of returning
    an iterator.
    """
    in_queue, _, dispatcher = start_dispatcher()
    conversation = IterStreamEvents("my-stream")
    first_msg = read_stream_events_failure(
        conversation.conversation_id, ReadStreamResult.StreamDeleted
    )

    future = await dispatcher.enqueue_conversation(conversation)

    await in_queue.put(first_msg)

    with pytest.raises(StreamDeleted):
        await asyncio.wait_for(future, 1)

    assert not dispatcher.has_conversation(conversation.conversation_id)
    dispatcher.stop()


@pytest.mark.asyncio
async def test_when_running_a_persistent_subscription():
    """
    We ought to be able to connect a persistent subscription, and receive
    some events.
    """
    in_queue, out_queue, dispatcher = start_dispatcher()
    conversation = ConnectPersistentSubscription("my-sub", "my-stream")
    first_msg = persistent_subscription_confirmed(
        conversation.conversation_id, "my-sub"
    )

    # The subscription confirmed message should result in our having a
    # PersistentSubscription to play with.

    future = await dispatcher.enqueue_conversation(conversation)
    await in_queue.put(first_msg)
    subscription = await asyncio.wait_for(future, 1)

    # A subsequent PersistentSubscriptionStreamEventAppeared message should
    # enqueue the event onto our iterator
    await in_queue.put(
        subscription_event_appeared(
            conversation.conversation_id, NewEvent("event", data={"x": 2})
        )
    )

    event = await anext(subscription.events)
    assert event.event.json()['x'] == 2

    # Remove the connet message
    await out_queue.get()

    # Acknowledging the event should place an Ack message on the out_queue

    await subscription.ack(event)

    expected_payload = proto.PersistentSubscriptionAckEvents()
    expected_payload.subscription_id = "my-sub"
    expected_payload.processed_event_ids.append(event.event.id.bytes_le)
    expected_message = OutboundMessage(
        conversation.conversation_id,
        TcpCommand.PersistentSubscriptionAckEvents,
        expected_payload.SerializeToString()
    )

    ack = await out_queue.get()

    assert ack == expected_message
    dispatcher.stop()


@pytest.mark.asyncio
async def test_when_a_persistent_subscription_fails_on_connection():
    """
    If a persistent subscription fails to connect we should raise the error
    to the caller. We're going to use an authentication error here and leave
    the vexing issue of NotHandled for another day.
    """
    in_queue, _, dispatcher = start_dispatcher()
    conversation = ConnectPersistentSubscription("my-sub", "my-stream")
    future = await dispatcher.enqueue_conversation(conversation)

    await in_queue.put(persistent_subscription_dropped(
        conversation.conversation_id, SubscriptionDropReason.AccessDenied
    ))

    with pytest.raises(SubscriptionCreationFailed):
        await asyncio.wait_for(future, 1)


@pytest.mark.asyncio
async def test_when_a_persistent_subscription_fails():
    """
    If a persistent subscription fails with something non-recoverable then
    we should raise the error to the caller
    """
    in_queue, _, dispatcher = start_dispatcher()
    conversation = ConnectPersistentSubscription("my-sub", "my-stream")
    future = await dispatcher.enqueue_conversation(conversation)

    await in_queue.put(persistent_subscription_confirmed(
        conversation.conversation_id, "my-sub"
    ))

    subscription = await asyncio.wait_for(future, 1)

    await in_queue.put(persistent_subscription_dropped(
        conversation.conversation_id, SubscriptionDropReason.AccessDenied
    ))

    with pytest.raises(SubscriptionFailed):
        await anext(subscription.events)
    dispatcher.stop()


@pytest.mark.asyncio
async def test_when_a_persistent_subscription_is_unsubscribed():
    """
    If a persistent subscription gets unsubscribed while processing
    we should log an info and exit gracefully
    """
    in_queue, _, dispatcher = start_dispatcher()
    conversation = ConnectPersistentSubscription("my-sub", "my-stream")
    future = await dispatcher.enqueue_conversation(conversation)

    await in_queue.put(persistent_subscription_confirmed(
        conversation.conversation_id, "my-sub"
    ))

    subscription = await asyncio.wait_for(future, 1)

    await in_queue.put(persistent_subscription_dropped(
        conversation.conversation_id, SubscriptionDropReason.Unsubscribed
    ))

    [] = [e async for e in subscription.events]
    dispatcher.stop()
