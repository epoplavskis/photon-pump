import json
from enum import IntEnum
from operator import attrgetter
from typing import Iterable, List, NamedTuple, Optional

import aiohttp
import pytest
from aioresponses import aioresponses

from . import data
from photonpump.discovery import *


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
        GOOD_NODE._replace(state=s)
        for s in
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
        mock.get('http://10.10.10.10:2113/gossip', status=200, payload=data.GOSSIP)
        session = aiohttp.ClientSession()
        gossip = await fetch_new_gossip(session, node)

    assert len(gossip) == 3


@pytest.mark.asyncio
async def test_aiohttp_failure():
    node = NodeService(address='10.10.10.10', port=2113, secure_port=None)

    with aioresponses() as mock:
        mock.get('http://10.10.10.10:2113/gossip', status=200, payload=data.GOSSIP)
        session = aiohttp.ClientSession()
        gossip = await fetch_new_gossip(session, node)

    assert len(gossip) == 3
