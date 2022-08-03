import time

from photonpump import messages, StreamNotFound, Client


async def given_a_stream_with_three_events(c, stream_name):
    result = await c.publish(
        stream_name,
        [
            messages.NewEvent(
                "pony_jumped",
                data={"Pony": "Derpy Hooves", "Height": 10, "Distance": 13},
            ),
            messages.NewEvent(
                "pony_jumped",
                data={"Pony": "Sparkly Hooves", "Height": 4, "Distance": 9},
            ),
            messages.NewEvent(
                "pony_jumped",
                data={"Pony": "Unlikely Hooves", "Height": 73, "Distance": 912},
            ),
        ],
    )
    assert "denied" not in str(result).lower()  # this should now never happen
    await wait_for_stream(c, stream_name)


async def given_two_streams_with_two_events(c, unique_id):
    await c.publish(
        f"stream_one_{unique_id}",
        [
            messages.NewEvent(
                "pony_splits",
                data={"Pony": "Derpy Hooves", "Height": 10, "Distance": 13},
            ),
            messages.NewEvent(
                "pony_splits",
                data={"Pony": "Sparkly Hooves", "Height": 4, "Distance": 9},
            ),
        ],
    )
    await c.publish(
        f"stream_two_{unique_id}",
        [
            messages.NewEvent(
                "pony_splits",
                data={"Pony": "Unlikely Hooves", "Height": 63, "Distance": 23},
            ),
            messages.NewEvent(
                "pony_splits",
                data={"Pony": "Glitter Hooves", "Height": 11, "Distance": 94},
            ),
        ],
    )
    await wait_for_stream(c, f"stream_one_{unique_id}")
    await wait_for_stream(c, f"stream_two_{unique_id}")


async def wait_for_stream(
    c: Client, stream: str, timeout: int = 0.1, attempts: int = 10
) -> None:
    for attempt in range(attempts):
        try:
            await c.get(stream=stream, max_count=1)
            return
        except StreamNotFound:
            if attempt + 1 == attempts:
                raise
            time.sleep(timeout)
