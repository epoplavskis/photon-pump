from photonpump import messages


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
