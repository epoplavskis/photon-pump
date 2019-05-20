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
        assert event.received_event.id == event_id


@pytest.mark.asyncio
async def test_subscribe_to(event_loop):

    async with connect(username="admin", password="changeit", loop=event_loop) as conn:
        stream_name = str(uuid.uuid4())
        event_id = uuid.uuid4()

        await conn.publish_event(stream_name, "my-event-type", id=event_id)

        subscription = await conn.subscribe_to(stream_name, start_from=0)

        event = await subscription.events.anext()
        assert event.received_event.id == event_id


# @pytest.mark.asyncio
# async def test_subscribe_to_all(event_loop):
#     async with connect(username="admin", password="changeit", loop=event_loop) as conn:
#         stream_name_1 = "pony_1"
#         event_id_1 = uuid.uuid4()

#         stream_name_2 = "pony_2"
#         event_id_2 = uuid.uuid4()

#         await conn.publish_event(stream_name_1, "my-event-type", id=event_id_1)
#         await conn.publish_event(stream_name_2, "my-event-type", id=event_id_2)

#         subscription = await conn.subscribe_to_all(start_from=0)
#         event_ids_list = []
#         async for event in subscription.events:
#             await event_ids_list.append(event.received_event.id)

#         assert True


# def new_event(conn):
#     stream_name_3 = "pony_3"
#     event_id_3 = uuid.uuid4()
#     conn.publish_event(stream_name_3, "my-event-type", id=event_id_3)


# def stop(subscription):
#     subscription.events.enqueue(StopAsyncIteration())


# async def connection_break(conn, subscription):
#     stream_name_3 = "pony_3"
#     event_id_3 = uuid.uuid4()
#     # await asyncio.sleep(1)
#     await conn.publish_event(stream_name_3, "my-event-type", id=event_id_3)
#     # await asyncio.sleep(5)
#     await subscription.events.enqueue(StopAsyncIteration())

