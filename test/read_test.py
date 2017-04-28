import photonpump
from photonpump import connect, messages, messages_pb2
import pytest
import uuid

@pytest.mark.asyncio
async def test_single_event_roundtrip(event_loop):

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop) as c:
        await c.publish_event('thing_happened', stream_name, body={
            'thing': 1,
            'happening': True
        })

        result = await c.get_event(stream_name, 0)

        assert isinstance(result, messages.Event)
        assert result.type == 'thing_happened'

        data = result.json()
        assert data['thing'] == 1
        assert data['happening'] == True

@pytest.mark.asyncio
async def test_missing_stream(event_loop):

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop) as c:

        exc = None

        try:
            result = await c.get_event(stream_name, 0)
        except Exception as e:
            exc = e

        assert isinstance(exc, photonpump.StreamNotFoundException)
        assert exc.stream == stream_name

@pytest.mark.asyncio
async def test_read_multiple(event_loop):

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop) as c:
        await c.publish(stream_name, [
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

        result = await c.get(stream_name)

        assert isinstance(result, list)
        assert len(result) == 3

        event = result[1]

        assert event.type == 'pony_jumped'

        data = event.json()
        assert data['Pony'] == 'Sparkly Hooves'
        assert data['Height'] == 4
