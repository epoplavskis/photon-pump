import logging
import uuid

import pytest

from photonpump import connect, exceptions, messages

from .fixtures import (
    given_a_stream_with_three_events,
    given_two_streams_with_two_events,
)


@pytest.mark.asyncio
async def test_single_event_roundtrip(event_loop):

    stream_name = str(uuid.uuid4())

    try:
        async with connect(loop=event_loop) as c:
            await c.publish_event(
                stream_name, "thing_happened", body={"thing": 1, "happening": True}
            )

            result = await c.get_event(stream_name, 0)

            assert isinstance(result, messages.Event)
            assert result.event.type == "thing_happened"

            data = result.event.json()
            assert data["thing"] == 1
            assert data["happening"] is True
    except Exception as e:
        print(e)
        assert False


@pytest.mark.asyncio
async def test_missing_stream(event_loop):

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop) as c:

        exc = None

        try:
            await c.get_event(stream_name, 0)
        except Exception as e:
            exc = e

        assert isinstance(exc, exceptions.StreamNotFound)
        assert exc.stream == stream_name


@pytest.mark.asyncio
async def test_read_multiple(event_loop):

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop) as c:

        await given_a_stream_with_three_events(c, stream_name)

        result = await c.get(stream_name)

        assert isinstance(result, messages.StreamSlice)
        assert len(result) == 3

        event = result[1]

        assert event.type == "pony_jumped"

        data = event.json()
        assert data["Pony"] == "Sparkly Hooves"
        assert data["Height"] == 4


@pytest.mark.asyncio
async def test_read_with_max_count(event_loop):

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop) as c:

        await given_a_stream_with_three_events(c, stream_name)

        result = await c.get(stream_name, max_count=1)

        assert isinstance(result, list)
        assert len(result) == 1

        event = result[0]

        assert event.type == "pony_jumped"

        data = event.json()
        assert data["Pony"] == "Derpy Hooves"


@pytest.mark.asyncio
async def test_read_with_max_count_and_from_event(event_loop):

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop, name="max-count-and-from") as c:

        await given_a_stream_with_three_events(c, stream_name)

        result = await c.get(stream_name, max_count=1, from_event=2)

        assert isinstance(result, list)
        assert len(result) == 1

        event = result[0]

        assert event.type == "pony_jumped"

        data = event.json()
        assert data["Pony"] == "Unlikely Hooves"


@pytest.mark.asyncio
async def test_streaming_read(event_loop):

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop, name="streaming-read") as c:

        await given_a_stream_with_three_events(c, stream_name)

        events_read = 0

        async for event in c.iter(stream_name, batch_size=1):
            logging.info("Handling event!")
            events_read += 1
            assert event.type == "pony_jumped"

        assert events_read == 3


@pytest.mark.asyncio
async def test_async_comprehension(event_loop):
    def embiggen(e):
        data = e.json()
        data["Height"] *= 10
        data["Distance"] *= 10

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop, name="comprehensions") as c:

        await given_a_stream_with_three_events(c, stream_name)

        jumps = (
            e.event
            async for e in c.iter(stream_name, batch_size=2)
            if e.type == "pony_jumped"
        )
        big_jumps = (embiggen(e) async for e in jumps)

        events_read = 0

        async for event in big_jumps:
            print(event)
            events_read += 1

        assert events_read == 3


@pytest.mark.asyncio
async def test_iter_from_missing_stream(event_loop):

    async with connect(loop=event_loop) as c:
        try:
            [e async for e in c.iter("my-stream-that-isnt-a-stream")]
            assert False
        except Exception as e:
            assert isinstance(e, exceptions.StreamNotFound)
