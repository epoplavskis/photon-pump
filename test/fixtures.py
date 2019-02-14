from photonpump import messages


async def given_a_stream_with_three_events(c, stream_name):
    await c.publish(
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

async def given_two_streams_with_two_events(c):
    await c.publish(
        "stream_one",
        [
            messages.NewEvent(
                "pony_jumped",
                data={"Pony": "Derpy Hooves", "Height": 10, "Distance": 13},
            ),
            messages.NewEvent(
                "pony_jumped",
                data={"Pony": "Sparkly Hooves", "Height": 4, "Distance": 9},
            ),
            
        ],
    )
    await c.publish(
        "stream_two",
        [
            messages.NewEvent(
                "pony_jumped",
                data={"Pony": "Unlikely Hooves", "Height": 63, "Distance": 23},
            ),
            messages.NewEvent(
                "pony_jumped",
                data={"Pony": "Glitter Hooves", "Height": 11, "Distance": 94},
            ),
            
        ],
    )
