import asyncio
import json
import logging
from enum import IntEnum
from operator import attrgetter
import random
from typing import Iterable, List, NamedTuple, Optional

import aiodns

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
    for elem in elems:
        return elem


def select(gossip: List[DiscoveredNode]) -> Optional[DiscoveredNode]:
    return first(
        sorted(
            [
                node for node in gossip
                if node.is_alive and node.state not in INELIGIBLE_STATE
            ],
            reverse=True,
            key=attrgetter('state')
        )
    )


def read_gossip(data):
    if not data:
        return []

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
            "Attempting to discover gossip nodes from DNS name %s; attempt %d of %d",
            name, current_attempt, max_attempt
        )
        try:
            result = await resolver.query(name, 'A')

            if result:
                current_attempt = 0

                for node in random.shuffle(result):
                    yield NodeService(
                        address=node.host, port=port, secure_port=None
                    )
        except aiodns.DNSError:
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

    resp = await session.get(f'http://{seed.address}:{seed.port}/gossip')
    data = await resp.json()

    return read_gossip(data)


async def discover_best_cluster_node(seed_finder):

    async for seed in seed_finder():
        gossip = fetch_new_gossip(seed)

        if gossip:
            return select(gossip)
