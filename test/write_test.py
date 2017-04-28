from photonpump import connect, messages, messages_pb2
import pytest
import uuid

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
