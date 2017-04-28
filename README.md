# photon-pump
A TCP eventstore client in Python 3.6

```python
async def write_an_event():
    async with photonpump.connect() as conn:
        await conn.publish_event('pony.jumped', body={
            'name': 'Applejack',
            'height_m': 0.6
        })
```
