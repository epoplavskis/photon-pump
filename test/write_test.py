from photonpump import connect, messages
import pytest
import uuid

@pytest.mark.asyncio
async def test_boop(event_loop):

    stream_name = str(uuid.uuid4())
    async with connect(loop=event_loop) as conn:
        result = await conn.publish_event(
            "testEvent",
            stream_name,
            eventId=uuid.uuid4(),
            body={}
            )

        assert isinstance(result, messages.WriteEventsCompleted)
        assert result.first_event_number == 0
        assert result.last_event_number == 0

        print(result.message)

