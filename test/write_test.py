import json
import uuid

import pytest

from photonpump import connect, messages, messages_pb2


@pytest.mark.asyncio
async def test_single_event_publish(event_loop):

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop) as conn:
        result = await conn.publish_event(
            "testEvent",
            stream_name,
            id=uuid.uuid4(),
            body={
                "greeting": "hello",
                "target": "world"
                }
            )

        assert isinstance(result, messages_pb2.WriteEventsCompleted)
        assert result.first_event_number == 0


@pytest.mark.asyncio
async def test_three_events_publish(event_loop):

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop) as c:

        result = await c.publish(stream_name, [
            messages.NewEvent('pony_jumped', data={
                "Pony": "Derpy Hooves",
                "Height": 10,
                "Distance": 13
            }),
            messages.NewEvent('pony_jumped', data={
                "Pony": "Sparkly Hooves",
                "Height": 4,
                "Distance": 9
            }),
            messages.NewEvent('pony_jumped', data={
                "Pony": "Unlikely Hooves",
                "Height": 73,
                "Distance": 912
            }),
            ])

        assert result.first_event_number == 0
        assert result.last_event_number == 2


@pytest.mark.asyncio
async def test_a_large_event(event_loop):

    stream_name = str(uuid.uuid4())

    with open('test/test-data/chair.json') as f:
        data = json.load(f)

    async with connect(loop=event_loop) as c:
        write_result = await c.publish(
            stream_name, [
                messages.NewEvent('big_json', data=data),
                messages.NewEvent('big_json', data=data),
                messages.NewEvent('big_json', data=data)
            ]
        )
        assert write_result.first_event_number == 0
        read_result = await c.get(stream_name, 0)
        print(read_result)
        assert read_result[0].event.type == 'big_json'
