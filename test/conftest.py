import asyncio
import logging

import pytest

logging.basicConfig(level=logging.DEBUG)


@pytest.fixture
def event_loop():

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
