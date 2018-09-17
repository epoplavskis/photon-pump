import asyncio
import logging
import random
import socket
from enum import IntEnum
from operator import attrgetter
from typing import Callable, Iterable, List, NamedTuple, Optional

import aiodns
import aiohttp

LOG = logging.getLogger("photonpump.discovery")


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


ELIGIBLE_STATE = [NodeState.Clone, NodeState.Slave, NodeState.Master]


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


Selector = Callable[[List[DiscoveredNode]], Optional[DiscoveredNode]]


def first(elems: Iterable):
    LOG.info(elems)

    for elem in elems:
        return elem


def prefer_master(nodes: List[DiscoveredNode]) -> Optional[DiscoveredNode]:
    """
    Select the master if available, otherwise fall back to a replica.
    """
    return max(nodes, key=attrgetter("state"))


def prefer_replica(nodes: List[DiscoveredNode]) -> Optional[DiscoveredNode]:
    """
    Select a random replica if any are available or fall back to the master.
    """
    masters = [node for node in nodes if node.state == NodeState.Master]
    replicas = [node for node in nodes if node.state != NodeState.Master]

    if replicas:
        return random.choice(replicas)
    else:
        # if you have more than one master then you're on your own, bud.

        return masters[0]


def select_random(nodes: List[DiscoveredNode]) -> Optional[DiscoveredNode]:
    """
    Return a random node.
    """
    return random.choice(nodes)


def select(
    gossip: List[DiscoveredNode], selector: Optional[Selector] = None
) -> Optional[DiscoveredNode]:
    eligible_nodes = [
        node for node in gossip if node.is_alive and node.state in ELIGIBLE_STATE
    ]

    LOG.debug("Selecting node from gossip members: %r", eligible_nodes)

    if not eligible_nodes:
        return None

    selector = selector or prefer_master

    return selector(eligible_nodes)


def read_gossip(data):
    if not data:
        LOG.debug("No gossip returned")

        return []

    LOG.debug("Received gossip for {%s} nodes", len(data["members"]))

    return [
        DiscoveredNode(
            state=NodeState[m["state"]],
            is_alive=m["isAlive"],
            internal_tcp=NodeService(m["internalTcpIp"], m["internalTcpPort"], None),
            external_tcp=NodeService(m["externalTcpIp"], m["externalTcpPort"], None),
            internal_http=NodeService(m["internalHttpIp"], m["internalHttpPort"], None),
            external_http=NodeService(m["externalHttpIp"], m["externalHttpPort"], None),
        )
        for m in data["members"]
    ]


class DiscoveryFailed(Exception):
    pass


class StaticSeedFinder:
    def __init__(self, seeds):
        self._seeds = list(seeds)
        self.candidates = []

    def reset_to_seeds(self):
        self.candidates = list(self._seeds)
        random.shuffle(self.candidates)

    def mark_failed(self, seed):
        self._seeds.remove(seed)

    async def next(self):
        if not self.candidates:
            self.reset_to_seeds()

        if not self.candidates:
            return None

        return self.candidates.pop()

    def add_node(self, node):
        self.candidates.append(node)


class DnsSeedFinder:
    def __init__(self, name, resolver, port=2113):
        self.name = name
        self.resolver = resolver
        self.port = port
        self.seeds = []
        self.failed_node = NodeService("_", 0, None)

    async def reset_to_dns(self):
        max_attempt = 100
        current_attempt = 0

        while current_attempt < max_attempt:
            LOG.info(
                "Attempting to discover gossip nodes from DNS name %s; "
                "attempt %d of %d",
                self.name,
                current_attempt,
                max_attempt,
            )
            try:
                result = await self.resolver.query(self.name, "A")
                random.shuffle(result)

                if result:
                    LOG.debug("Found %s hosts for name %s", len(result), self.name)
                    current_attempt = 0
                    self.seeds = [
                        NodeService(address=node.host, port=self.port, secure_port=None)
                        for node in result
                        if node.host != self.failed_node.address
                    ]

                    break
            except aiodns.error.DNSError:
                LOG.warning(
                    "Failed to fetch gossip seeds for dns name %s",
                    self.name,
                    exc_info=True,
                )
            current_attempt += 1
            await asyncio.sleep(1)

    def mark_failed(self, seed):
        if seed in self.seeds:
            self.seeds.remove(seed)
        self.failed_node = seed

    async def next(self):
        if not self.seeds:
            await self.reset_to_dns()

        if not self.seeds:
            return None

        return self.seeds[0]

    def add_node(self, node):
        self.seeds.append(node)


async def fetch_new_gossip(session, seed):
    if not seed:
        return []

    LOG.debug("Fetching gossip from http://%s:%s/gossip", seed.address, seed.port)
    try:
        resp = await session.get(f"http://{seed.address}:{seed.port}/gossip")
        data = await resp.json()

        return read_gossip(data)
    except aiohttp.ClientError:
        LOG.exception(
            "Failed loading gossip from http://%s:%s/gossip", seed.address, seed.port
        )

        return None


class SingleNodeDiscovery:
    def __init__(self, node):
        self.node = node
        self.failed = False

    def mark_failed(self, node):
        if node == self.node:
            self.failed = True

    async def discover(self):
        if self.failed:
            raise DiscoveryFailed()
        LOG.debug("SingleNodeDiscovery returning node %s", self.node)

        return self.node


class DiscoveryStats(NamedTuple):

    node: NodeService
    attempts: int
    successes: int
    failures: int
    consecutive_failures: int


class Stats(dict):
    def __missing__(self, key):
        value = self[key] = DiscoveryStats(key, 0, 0, 0, 0)

        return value

    def record_success(self, node):
        val = self[node]
        self[node] = val._replace(
            attempts=(val.attempts + 1),
            successes=(val.successes + 1),
            consecutive_failures=0,
        )

    def record_failure(self, node):
        val = self[node]
        self[node] = val._replace(
            attempts=(val.attempts + 1),
            failures=(val.failures + 1),
            consecutive_failures=(val.consecutive_failures + 1),
        )


class ClusterDiscovery:
    def __init__(self, seed_finder, http_session, retry_policy, selector):
        self.session = http_session
        self.seeds = seed_finder
        self.last_gossip = []
        self.best_node = None
        self.retry_policy = retry_policy
        self.selector = selector

    def close(self):
        self.session.close()

    def mark_failed(self, node):
        self.seeds.mark_failed(node)

    def record_gossip(self, node, gossip):
        self.last_gossip = gossip

        for member in gossip:
            self.seeds.add_node(member.external_http)
        self.best_node = select(gossip, self.selector)
        self.retry_policy.record_success(node)

    async def get_gossip(self):
        while True:

            seed = await self.seeds.next()
            LOG.info(f"Found gossip seed {seed}")

            if not seed:
                raise DiscoveryFailed()

            await self.retry_policy.wait(seed)
            gossip = await fetch_new_gossip(self.session, seed)

            if gossip:
                self.record_gossip(seed, gossip)

                return gossip
            else:
                self.retry_policy.record_failure(seed)

                if not self.retry_policy.should_retry(seed):
                    self.seeds.mark_failed(seed)

    async def discover(self):
        gossip = await self.get_gossip()

        if gossip:
            if self.best_node:
                return self.best_node.external_tcp
        raise DiscoveryFailed()


class DiscoveryRetryPolicy:
    def __init__(
        self,
        retries_per_node=3,
        retry_interval=0.5,
        jitter=0.5,
        multiplier=1.5,
        max_interval=60,
    ):
        self.stats = Stats()
        self.retries_per_node = retries_per_node
        self.retry_interval = retry_interval
        self.jitter = jitter
        self.multiplier = multiplier
        self.max_interval = max_interval
        self.next_interval = self.retry_interval

    def should_retry(self, node):
        stats = self.stats[node]

        return stats.consecutive_failures < self.retries_per_node

    async def wait(self, seed):
        stats = self.stats[seed]

        if stats.consecutive_failures == 0:
            return

        next_interval = (
            self.retry_interval * self.multiplier * stats.consecutive_failures
        )
        maxinterval = next_interval + self.jitter
        mininterval = next_interval - self.jitter
        interval = random.uniform(mininterval, maxinterval)

        LOG.debug(f"Discovery retry policy sleeping for {interval} secs")
        await asyncio.sleep(interval)

    def record_success(self, node):
        self.stats.record_success(node)
        self.next_interval = self.retry_interval

    def record_failure(self, node):
        self.stats.record_failure(node)


def get_discoverer(
    host, port, discovery_host, discovery_port, selector: Optional[Selector] = None
):
    if discovery_host is None:
        LOG.info("Using single-node discoverer")

        return SingleNodeDiscovery(NodeService(host or "localhost", port, None))

    session = aiohttp.ClientSession()
    try:
        socket.inet_aton(discovery_host)
        LOG.info("Using cluster node discovery with a static seed")

        return ClusterDiscovery(
            StaticSeedFinder([NodeService(discovery_host, discovery_port, None)]),
            session,
            DiscoveryRetryPolicy(),
            selector,
        )
    except socket.error:
        LOG.info("Using cluster node discovery with DNS")
        resolver = aiodns.DNSResolver()

        return ClusterDiscovery(
            DnsSeedFinder(discovery_host, resolver, discovery_port),
            session,
            DiscoveryRetryPolicy(),
            selector,
        )
