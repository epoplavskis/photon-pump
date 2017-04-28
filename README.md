# photon-pump
A TCP eventstore client in Python 3.6

```python
async def write_an_event():
    async with photonpump.connect() as conn:
        await conn.publish_event('pony_stream', 'pony.jumped', body={
            'name': 'Applejack',
            'height_m': 0.6
        })


async def read_an_event(conn):
    event = await conn.get_event('pony_stream', 1)
    print(event)


async def ticker(delay, to):
    for i in range(to):
        yield NewEvent('tick', body{ 'tick': i})
        await asyncio.sleep(delay)


async def write_an_infinite_number_of_events(conn):
    await conn.publish('ticker_stream', ticker(1000))


async def read_an_infinite_number_of_events(conn):
    async for event in conn.stream('ticker_stream'):
        print(event)
```
