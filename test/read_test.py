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

        result = await c.get(stream_name, 0)

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
            result = await c.get(stream_name, 0)
        except Exception as e:
            exc = e

        assert isinstance(exc, photonpump.StreamNotFoundException)
        assert exc.stream == stream_name
