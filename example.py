import asyncio

import photonpump


async def write_an_event(conn):
    await conn.publish_event(
        'pony_stream',
        'pony.jumped',
        body={
            'name': 'Applejack',
            'height_m': 0.6
        }
    )


async def read_an_event(conn):
    event = await conn.get_event('pony_stream', 0)
    print(event)


async def do_things():
    async with photonpump.connect() as conn:
        await write_an_event(conn)
        await read_an_event(conn)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(do_things())
