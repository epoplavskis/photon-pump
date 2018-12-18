import uuid

import pytest

from photonpump import connect


@pytest.mark.asyncio
async def test_ping_context_mgr(event_loop):

    async with connect(loop=event_loop) as conn:
        id = uuid.uuid4()
        await conn.ping(conversation_id=id)


@pytest.mark.asyncio
async def test_connect_subscription(event_loop):

    async with connect(username="admin", password="changeit", loop=event_loop) as conn:
        subscription_name = str(uuid.uuid4())
        stream_name = str(uuid.uuid4())
        event_id = uuid.uuid4()

        await conn.create_subscription(subscription_name, stream_name, start_from=-1)
        subscription = await conn.connect_subscription(subscription_name, stream_name)
        await conn.publish_event(stream_name, "my-event-type", id=event_id)

        event = await subscription.events.anext()
        assert event.original_event_id == event_id


@pytest.mark.asyncio
async def test_subscribe_to(event_loop):

    async with connect(username="admin", password="changeit", loop=event_loop) as conn:
        stream_name = str(uuid.uuid4())
        event_id = uuid.uuid4()

        subscription = await conn.subscribe_to(stream_name)

        await conn.publish_event(stream_name, "my-event-type", id=event_id)

        event = await subscription.events.anext()
        assert event.original_event_id == event_id
