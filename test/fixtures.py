from photonpump import messages


async def given_a_stream_with_three_events(c, stream_name):
    await c.publish(
        stream_name, [
            messages.NewEvent('pony_jumped', data={
                "Pony": "Derpy Hooves",
                "Height": 10,
                "Distance": 13
            }),
            messages.NewEvent('pony_jumped', data={
                "Pony": "Sparkly Hooves",
                "Height": 4,
                "Distance": 9
            }),
            messages.NewEvent('pony_jumped', data={
                "Pony": "Unlikely Hooves",
                "Height": 73,
                "Distance": 912
            }),
        ]
    )
