import pytest

from photonpump import connect
from photonpump.discovery import DiscoveryRetryPolicy


@pytest.mark.asyncio
async def test_setting_retry_policy(event_loop):

    class silly_retry_policy(DiscoveryRetryPolicy):
        def __init__(self):
            super().__init__()

        def should_retry(self, _):
            pass

        async def wait(self, seed):
            pass

    expected_policy = silly_retry_policy()

    async with connect(loop=event_loop, retry_policy=expected_policy) as client:
        assert client.connector.discovery.retry_policy == expected_policy

