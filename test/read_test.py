import photonpump
from photonpump import connect, messages, messages_pb2
import pytest
import uuid

from .fixtures import given_a_stream_with_three_events

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

        await given_a_stream_with_three_events(c, stream_name)

        result = await c.get(stream_name)

        assert isinstance(result, list)
        assert len(result) == 3

        event = result[1]

        assert event.type == 'pony_jumped'

        data = event.json()
        assert data['Pony'] == 'Sparkly Hooves'
        assert data['Height'] == 4


@pytest.mark.asyncio
async def test_read_with_max_count(event_loop):

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop) as c:

        await given_a_stream_with_three_events(c, stream_name)

        result = await c.get(stream_name, max_count=1)

        assert isinstance(result, list)
        assert len(result) == 1

        event = result[0]

        assert event.type == 'pony_jumped'

        data = event.json()
        assert data['Pony'] == 'Derpy Hooves'

@pytest.mark.asyncio
async def test_read_with_max_count_and_from_event(event_loop):

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop) as c:

        await given_a_stream_with_three_events(c, stream_name)

        result = await c.get(stream_name, max_count=1, from_event=2)

        assert isinstance(result, list)
        assert len(result) == 1

        event = result[0]

        assert event.type == 'pony_jumped'

        data = event.json()
        assert data['Pony'] == 'Unlikely Hooves'

@pytest.mark.asyncio
async def test_streaming_read(event_loop):

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop) as c:

        await given_a_stream_with_three_events(c, stream_name)

        events_read = 0

        async for event in c.stream(stream_name, batch_size=1):
            events_read += 1
            assert event.type == 'pony_jumped'

        assert events_read == 3


