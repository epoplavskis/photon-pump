import uuid

import pytest

from photonpump import Client, connect


@pytest.mark.asyncio
async def test_ping_context_mgr(event_loop):

    async with connect(loop=event_loop) as conn:
        id = uuid.uuid4()
        await conn.ping(conversation_id=id)


@pytest.mark.asyncio
async def test_connect_subscription(event_loop):

    async with connect(username='admin', password='changeit',
                       loop=event_loop) as conn:
        subscription_name = str(uuid.uuid4())
        await conn.create_subscription(subscription_name, 'ping', start_from=-1)

        await conn.connect_subscription(subscription_name, 'ping')
