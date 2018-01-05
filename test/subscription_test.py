from asyncio import Future

import pytest

from photonpump import CreateVolatileSubscription

class FakeWriter:

    def __init__(self):
        self.chunks = []

    def write(self, chunk):
        self.chunks.append(chunk)

@pytest.mark.asyncio
async def test_connection(event_loop):
   subscription = CreateVolatileSubscription(
        'my-stream'
   )

