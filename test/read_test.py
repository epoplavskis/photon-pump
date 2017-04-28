from photonpump import connect, messages, messages_pb2
import pytest
import uuid

@pytest.mark.asyncio
async def test_single_event_roundtrip(event_loop):

    stream_name = str(uuid.uuid4())

    async with connect(loop=event_loop) as c:
        await c.publish_event('thing_happened', stream_name)

        result = await c.get(stream_name, 0)
        assert isinstance(result, messages.Event)

        assert result.type == 'thing_happened'
