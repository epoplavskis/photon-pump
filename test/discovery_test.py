import json
from enum import IntEnum
from operator import attrgetter
from typing import Iterable, List, NamedTuple, Optional

import aiohttp
import pytest
from aioresponses import aioresponses
from tenacity.retry import retry_never

from photonpump.discovery import (
    ClusterDiscovery, DiscoveredNode, DiscoveryFailed, DiscoveryStats,
    NodeService, NodeState, StaticSeedFinder, fetch_new_gossip, get_discoverer,
    read_gossip, select, Stats, DiscoveryRetryPolicy
)

from . import data

GOOD_NODE = DiscoveredNode(
    state=NodeState.Master,
    is_alive=True,
    internal_tcp=None,
    internal_http=None,
    external_http=None,
    external_tcp=NodeService('10.128.10.10', 1113, None)
)


def test_selector_with_one_node():

    gossip = [GOOD_NODE]

    selected = select(gossip)

    assert selected == GOOD_NODE


def test_selector_with_empty_gossip():

    gossip = []
    selected = select(gossip)
    assert selected is None


def test_selector_with_a_dead_node():

    gossip = [GOOD_NODE._replace(is_alive=False)]

    selected = select(gossip)

    assert selected is None


def test_selector_with_nodes_in_bad_states():

    gossip = [
        GOOD_NODE._replace(state=s) for s in
        [NodeState.Manager, NodeState.Shutdown, NodeState.ShuttingDown]
    ]

    selected = select(gossip)

    assert selected is None


def test_selector_with_nodes_in_all_states():
    gossip = [GOOD_NODE._replace(state=s) for s in NodeState]

    selected = select(gossip)

    assert selected.state == NodeState.Master


def test_selector_with_slave_and_clone():
    gossip = [
        GOOD_NODE._replace(state=NodeState.Clone),
        GOOD_NODE._replace(state=NodeState.Slave)
    ]

    selected = select(gossip)

    assert selected.state == NodeState.Slave


def test_selector_with_master_and_slave():
    gossip = [
        GOOD_NODE._replace(state=NodeState.Master),
        GOOD_NODE._replace(state=NodeState.Slave)
    ]

    selected = select(gossip)

    assert selected.state == NodeState.Master


def gossip_nodes(nodes: List[NodeService]):

    async def foo():
        return nodes

    return foo


@pytest.mark.asyncio
async def test_no_gossip_seeds_found():
    async with aiohttp.ClientSession() as session:
        gossip = await fetch_new_gossip(session, [])

    assert gossip == []


def test_gossip_reader():
    gossip = read_gossip(data.GOSSIP)
    assert len(gossip) == 3

    assert gossip[1].internal_tcp.port == 1112
    assert gossip[2].external_http.port == 2113
    assert gossip[0].internal_http.address == '172.31.224.200'


@pytest.mark.asyncio
async def test_fetch_gossip():
    node = NodeService(address='10.10.10.10', port=2113, secure_port=None)

    with aioresponses() as mock:
        mock.get(
            'http://10.10.10.10:2113/gossip', status=200, payload=data.GOSSIP
        )
        session = aiohttp.ClientSession()
        gossip = await fetch_new_gossip(session, node)

    assert len(gossip) == 3


@pytest.mark.asyncio
async def test_aiohttp_failure():
    node = NodeService(address='10.10.10.10', port=2113, secure_port=None)

    with aioresponses() as mock:
        mock.get('http://10.10.10.10:2113/gossip', status=502)
        session = aiohttp.ClientSession()
        gossip = await fetch_new_gossip(session, node)

    assert not gossip


@pytest.mark.asyncio
async def test_discovery_with_a_single_node():
    """
    When we create a discoverer with a single address and no
    discovery information, we should receive an async generator
    that returns the same node over and over.
    """
    discoverer = get_discoverer('localhost', 1113, None, None)

    for i in range(0, 5):
        assert discoverer.discover() == NodeService('localhost', 1113, None)


@pytest.mark.asyncio
async def test_discovery_with_a_static_seed():
    """
    When we create a discoverer with a static seed address we
    should query that address for gossip and then select the
    best node from the result.

    On a subsequent call, we should use the previously discovered
    nodes to find gossip.
    """

    seed_ip = '10.10.10.10'
    first_node_ip = '172.31.0.1'
    second_node_ip = '192.168.168.192'

    first_gossip = data.make_gossip(first_node_ip)
    second_gossip = data.make_gossip(second_node_ip)

    discoverer = get_discoverer(None, None, seed_ip, 2113)
    with aioresponses() as mock:
        mock.get(
            f'http://{seed_ip}:2113/gossip', status=200, payload=first_gossip
        )

        mock.get(
            f'http://{first_node_ip}:2113/gossip',
            status=200,
            payload=second_gossip
        )

        assert await discoverer.discover() == NodeService(
            first_node_ip, 1113, None
        )
        assert await discoverer.discover() == NodeService(
            second_node_ip, 1113, None
        )


@pytest.mark.asyncio
async def test_discovery_failure_for_static_seed():
    """
    When gossip fetch fails for a static seed, we should call the retry thing
    """

    class always_succeed(DiscoveryRetryPolicy):

        def __init__(self):
            self.stats = Stats()

        async def should_retry(self, _):
            return True

        async def wait(self):
            pass

    seed = NodeService('1.2.3.4', 2113, None)
    gossip = data.make_gossip('2.3.4.5')
    retry = always_succeed()
    with aioresponses() as mock:
        successful_discoverer = ClusterDiscovery(
            StaticSeedFinder([seed]), aiohttp.ClientSession(), retry
        )

        mock.get('http://1.2.3.4:2113/gossip', status=500)
        mock.get('http://1.2.3.4:2113/gossip', payload=gossip)

        assert await successful_discoverer.discover() == NodeService(
            '2.3.4.5', 1113, None
        )
        stats = retry.stats[seed]

        assert stats.attempts == 2
        assert stats.successes == 1
        assert stats.failures == 1
        assert stats.consecutive_failures == 0


@pytest.mark.asyncio
async def test_repeated_discovery_failure_for_static_seed():
    """
    When gossip fetch fails `maximum_retry_count` times, we should fail with a
    DiscoverFailed error.
    """

    class always_fail(DiscoveryRetryPolicy):

        async def should_retry(self, _):
            return False

        async def wait(self):
            pass

    seed = NodeService('1.2.3.4', 2113, None)
    retry = always_fail()
    gossip = data.make_gossip('2.3.4.5')
    with aioresponses() as mock:
        successful_discoverer = ClusterDiscovery(
            StaticSeedFinder([seed]), aiohttp.ClientSession(), retry
        )

        mock.get('http://1.2.3.4:2113/gossip', status=500)
        mock.get('http://1.2.3.4:2113/gossip', payload=gossip)

        with pytest.raises(DiscoveryFailed):
            assert await successful_discoverer.discover() == NodeService(
                '2.3.4.5', 1113, None
            )
            stats = successful_discoverer.stats[seed]

            assert stats.attempts == 1
            assert stats.successes == 0
            assert stats.failures == 1
            assert stats.consecutive_failures == 1


@pytest.mark.asyncio
async def test_discovery_with_dns():
    """
    When we provide a domain name as the gossip seed, we should resolve
    the name to a set of IP addresses and query them for gossip.

    On a subsequent call, we should ask the known instances for new gossip.
    again.
    """

    pass


@pytest.mark.asynio
async def test_gossip_failures_under_dns_discovery():
    """
    When we fail to fetch gossip for seeds obtained via DNS, we should retry
    each seed with a backoff, and then perform DNS discovery again
    """

    pass


@pytest.mark.asyncio
async def test_dns_partial_failure():
    """
    When the dns resolution fails, we should retry with a backoff.
    If we reach our maximum retry, we should raise DiscoveryFailed
    """

    pass
