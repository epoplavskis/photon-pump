# pylint: disable=invalid-name, bad-continuation, expression-not-assigned
import logging
import uuid
import pytest
from photonpump import connect, exceptions, messages
from .fixtures import (
    given_two_streams_with_two_events,
    given_a_stream_with_three_events,
)


@pytest.mark.asyncio
async def test_single_event_roundtrip(event_loop):
    stream_name = str(uuid.uuid4())
    async with connect(
        loop=event_loop, username="test-user", password="test-password"
    ) as c:
        await c.publish_event(
            stream_name, "thing_happened", body={"thing": 1, "happening": True}
        )

        result = await c.get_event(stream_name, 0)

        assert isinstance(result, messages.Event)
        assert result.event.type == "thing_happened"

        data = result.event.json()
        assert data["thing"] == 1
        assert data["happening"] is True


@pytest.mark.asyncio
async def test_missing_stream(event_loop):
    stream_name = str(uuid.uuid4())
    async with connect(
        loop=event_loop, username="test-user", password="test-password"
    ) as c:
        with pytest.raises(exceptions.StreamNotFound) as exc:
            await c.get_event(stream_name, 0)
        assert exc.value.stream == stream_name


@pytest.mark.asyncio
async def test_read_multiple(event_loop):
    stream_name = str(uuid.uuid4())
    async with connect(
        loop=event_loop, username="test-user", password="test-password"
    ) as c:
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
    async with connect(
        loop=event_loop, username="test-user", password="test-password"
    ) as c:
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
    async with connect(
        loop=event_loop, username="test-user", password="test-password"
    ) as c:
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
    async with connect(
        loop=event_loop,
        username="test-user",
        password="test-password",
        name="streaming-read",
    ) as c:
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

    async with connect(
        loop=event_loop,
        username="test-user",
        password="test-password",
        name="comprehensions",
    ) as c:

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
    async with connect(
        loop=event_loop, username="test-user", password="test-password"
    ) as c:
        with pytest.raises(exceptions.StreamNotFound):
            [e async for e in c.iter("my-stream-that-isnt-a-stream")]


@pytest.mark.asyncio
async def test_iterall(event_loop):
    async with connect(
        loop=event_loop,
        username="admin",  # iter_all aggregates all streams so it needs systemwide-read perms
        password="changeit",
        name="iter_all",
    ) as c:
        stream_name = str(uuid.uuid4())
        await given_two_streams_with_two_events(c, stream_name)

        events_read = 0

        async for _ in c.iter_all(batch_size=2):
            events_read += 1

        assert events_read >= 3


@pytest.mark.asyncio
async def test_readall(event_loop):
    async with connect(
        loop=event_loop,
        username="admin",  # get_all aggregates all streams so it needs systemwide-read perms
        password="changeit",
        name="iter_all",
    ) as c:
        stream_name = str(uuid.uuid4())
        await given_two_streams_with_two_events(c, stream_name)

        events_read = 0

        for event in await c.get_all(max_count=4):
            print(event)
            events_read += 1

        assert events_read == 4


@pytest.mark.asyncio
async def test_anonymous_access_still_works(event_loop):
    stream_name = str(uuid.uuid4())
    async with connect(loop=event_loop, port=11113, discovery_port=22113) as c:
        await c.publish_event(stream_name, "first_event", body={"thing": 1})
        results = await c.get(stream_name)
        assert results[0].event.type == "first_event"
