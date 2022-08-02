import asyncio
import logging
import ssl

import pytest
from photonpump import connect as _connect

logging.basicConfig(level=logging.DEBUG)


@pytest.fixture
def event_loop():

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture
def ssl_context() -> ssl.SSLContext:
    context = ssl.SSLContext()
    context.load_verify_locations("certs/ca/ca.crt")
    context.verify_mode = ssl.CERT_REQUIRED
    return context


@pytest.fixture
def connect(ssl_context):
    return lambda *args, **kwargs: _connect(
        *args, **{"sslcontext": ssl_context, "port": 1111, **kwargs}
    )
