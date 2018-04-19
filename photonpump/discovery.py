import asyncio
import json
import logging
import random
import socket
from enum import IntEnum
from operator import attrgetter
from typing import Iterable, List, NamedTuple, Optional

import aiodns
import aiohttp

LOG = logging.getLogger('photonpump.discovery')


class NodeState(IntEnum):
    Initializing = 0
    Unknown = 1
    PreReplica = 2
    CatchingUp = 3
    Clone = 4
    Slave = 5
    PreMaster = 6
    Master = 7
    Manager = 8
    ShuttingDown = 9
    Shutdown = 10


INELIGIBLE_STATE = [
    NodeState.Manager, NodeState.ShuttingDown, NodeState.Shutdown
]


class NodeService(NamedTuple):
    address: str
    port: int
    secure_port: Optional[int]


class DiscoveredNode(NamedTuple):

    state: NodeState
    is_alive: bool

    internal_tcp: NodeService
    external_tcp: NodeService

    internal_http: NodeService
    external_http: NodeService


def first(elems: Iterable):
    LOG.info(elems)

    for elem in elems:
        return elem


def select(gossip: List[DiscoveredNode]) -> Optional[DiscoveredNode]:
    eligible_nodes = [
        node for node in gossip
        if node.is_alive and node.state not in INELIGIBLE_STATE
    ]

    if not eligible_nodes:
        return None

    return max(eligible_nodes, key=attrgetter('state'))


def read_gossip(data):
    if not data:
        LOG.debug("No gossip returned")

        return []

    LOG.debug(f"Received gossip for { len(data['members']) } nodes")

    return [
        DiscoveredNode(
            state=m['state'],
            is_alive=m['isAlive'],
            internal_tcp=NodeService(
                m['internalTcpIp'], m['internalTcpPort'], None
            ),
            external_tcp=NodeService(
                m['externalTcpIp'], m['externalTcpPort'], None
            ),
            internal_http=NodeService(
                m['internalHttpIp'], m['internalHttpPort'], None
            ),
            external_http=NodeService(
                m['externalHttpIp'], m['externalHttpPort'], None
            )
        ) for m in data['members']
    ]


class DiscoveryFailed(Exception):
    pass


async def dns_seed_finder(resolver, name, port='2113'):

    max_attempt = 100
    current_attempt = 0

    while current_attempt < max_attempt:
        LOG.info(
            "Attempting to discover gossip nodes from DNS name %s; "
            "attempt %d of %d", name, current_attempt, max_attempt
        )
        try:
            result = await resolver.query(name, 'A')
            random.shuffle(result)

            if result:
                LOG.debug(f"Found { len(result) } hosts for name {name}")
                current_attempt = 0

                for node in result:
                    yield NodeService(
                        address=node.host, port=port, secure_port=None
                    )
        except aiodns.error.DNSError:
            LOG.warning(
                "Failed to fetch gossip seeds for dns name %s",
                name,
                exc_info=True
            )
        current_attempt += 1
        await asyncio.sleep(1)

    raise DiscoveryFailed()


async def static_seed_finder(nodes):
    for node in random.shuffle(nodes):
        yield node


async def fetch_new_gossip(session, seed):
    if not seed:
        return []

    LOG.debug(f"Fetching gossip from http://{seed.address}:{seed.port}/gossip")
    try:
        resp = await session.get(f'http://{seed.address}:{seed.port}/gossip')
        data = await resp.json()

        return read_gossip(data)
    except:
        LOG.exception(
            "Failed loading gossip from http://{seed.address}:{seed.port}/gossip"
        )


def discover_best_cluster_node(seed_finder, session):

    async def _discover():
        async for seed in seed_finder:
            gossip = await fetch_new_gossip(session, seed)

            if gossip:
                return select(gossip).external_tcp

    return _discover


def discover_single_node(node):

    async def _discover():
        return node

    return _discover


def get_discoverer(host, port, discovery_host, discovery_port):
    if discovery_host is None:
        LOG.info("Using single-node discoverer")

        return discover_single_node(
            NodeService(host or 'localhost', port, None)
        )

    session = aiohttp.ClientSession()
    try:
        socket.inet_aton(discovery_host)
        LOG.info("Using cluster node discovery with a static seed")

        return discover_best_cluster_node(
            static_seed_finder(
                [NodeService(discovery_host, discovery_port, None)]
            ), session
        )
    except socket.error:
        LOG.info("Using cluster node discovery with DNS")
        resolver = aiodns.DNSResolver()

        return discover_best_cluster_node(
            dns_seed_finder(resolver, discovery_host, discovery_port), session
        )
