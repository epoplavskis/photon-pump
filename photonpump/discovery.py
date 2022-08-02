import asyncio
import enum
import logging
import random
import socket
import ssl
from enum import IntEnum
from operator import attrgetter
from typing import Callable, Iterable, List, NamedTuple, Optional, Union

import aiodns
import aiohttp

LOG = logging.getLogger("photonpump.discovery")


class NodeState(IntEnum):
    Initializing = enum.auto()
    Unknown = enum.auto()
    PreReplica = enum.auto()
    CatchingUp = enum.auto()
    Clone = enum.auto()
    Follower = enum.auto()
    PreMaster = enum.auto()
    Leader = enum.auto()
    Manager = enum.auto()
    ShuttingDown = enum.auto()
    Shutdown = enum.auto()


ELIGIBLE_STATE = [NodeState.Clone, NodeState.Follower, NodeState.Leader]
KEEP_RETRYING = -1


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


def prefer_leader(nodes: List[DiscoveredNode]) -> Optional[DiscoveredNode]:
    """
    Select the master if available, otherwise fall back to a replica.
    """
    return max(nodes, key=attrgetter("state"))


def prefer_replica(nodes: List[DiscoveredNode]) -> Optional[DiscoveredNode]:
    """
    Select a random replica if any are available or fall back to the master.
    """
    leaders = [node for node in nodes if node.state == NodeState.Leader]
    replicas = [node for node in nodes if node.state != NodeState.Leader]

    if replicas:
        return random.choice(replicas)
    else:
        # if you have more than one master then you're on your own, bud.

        return leaders[0]


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

    selector = selector or prefer_leader
    LOG.debug(
        "Selecting node using '%s' from gossip members: %r", selector, eligible_nodes
    )

    if not eligible_nodes:
        return None

    return selector(eligible_nodes)


def read_gossip(data, secure: bool):
    if not data:
        LOG.debug("No gossip returned")

        return []

    LOG.debug("Received gossip for {%s} nodes", len(data["members"]))


    nodes = []
    for m in data["members"]:
        internal_tcp_port = m['internalSecureTcpPort'] if secure else m['internalTcpPort']
        external_tcp_port = m['externalSecureTcpPort'] if secure else m['externalTcpPort']
        internal_http_ip = m.get('httpEndPointIp', m.get('internalHttpIp'))
        assert internal_http_ip
        external_http_ip = m.get('httpEndPointIp', m.get('externalHttpIp'))
        assert external_http_ip
        internal_http_port = m.get('httpEndPointPort', m.get('internalHttpPort'))
        assert internal_http_port
        external_http_port = m.get('httpEndPointPort', m.get('externalHttpPort'))
        assert external_http_port
        node = DiscoveredNode(
            state=NodeState[m["state"]],
            is_alive=m["isAlive"],
            internal_tcp=NodeService(m["internalTcpIp"], internal_tcp_port, None),
            external_tcp=NodeService(m["externalTcpIp"], external_tcp_port, None),
            internal_http=NodeService(internal_http_ip, internal_http_port, None),
            external_http=NodeService(external_http_ip, external_http_port, None),
        )

        nodes.append(node)
    return nodes


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


async def fetch_new_gossip(session, seed, sslcontext):
    if not seed:
        return []

    url = f"{'https' if sslcontext else 'http'}://{seed.address}:{seed.port}/gossip"

    LOG.debug("Fetching gossip from %s", url)
    try:
        resp = await session.get(url, ssl=sslcontext)
        data = await resp.json()

        return read_gossip(data, bool(sslcontext))
    except aiohttp.ClientError:
        LOG.exception(
            "Failed loading gossip from %s", url
        )

        return None


class SingleNodeDiscovery:
    def __init__(self, node: NodeService, retry_policy):
        self.node = node
        self.failed = False
        self.retry_policy = retry_policy

    def record_failure(self, node):
        self.retry_policy.record_failure(node)

    def record_success(self, node):
        self.retry_policy.record_success(node)

    async def next_node(self):
        if self.retry_policy.should_retry(self.node):
            await self.retry_policy.wait(self.node)
            return self.node
        raise DiscoveryFailed()


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
    def __init__(self, seed_finder, retry_policy, selector, sslcontext):
        self.seeds = seed_finder
        self.last_gossip = []
        self.best_node = None
        self.retry_policy = retry_policy
        self.selector = selector
        self.sslcontext = sslcontext

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
            async with aiohttp.ClientSession() as session:
                gossip = await fetch_new_gossip(session, seed, self.sslcontext)

            if gossip:
                self.record_gossip(seed, gossip)

                return gossip
            else:
                self.retry_policy.record_failure(seed)

                if not self.retry_policy.should_retry(seed):
                    self.seeds.mark_failed(seed)

    async def next_node(self):
        gossip = await self.get_gossip()

        if gossip:
            if self.best_node:
                return self.best_node.external_tcp
        raise DiscoveryFailed()

    def record_failure(self, node):
        self.retry_policy.record_failure(node)

    def record_success(self, node):
        self.retry_policy.record_success(node)


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
        return (
            self.retries_per_node == KEEP_RETRYING
            or stats.consecutive_failures < self.retries_per_node
        )

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
    host: str,
    port: int,
    discovery_host: Optional[str],
    discovery_port: Optional[int],
    selector: Optional[Selector] = None,
    retry_policy: Optional[DiscoveryRetryPolicy] = None,
    sslcontext: Union[bool, None, ssl.SSLContext] = None,
):
    if discovery_host is None:
        LOG.info("Using single-node discoverer")

        retry_policy = retry_policy or DiscoveryRetryPolicy(
            retries_per_node=KEEP_RETRYING
        )
        return SingleNodeDiscovery(
            NodeService(host or "localhost", port, None), retry_policy
        )

    try:
        socket.inet_aton(discovery_host)
        LOG.info("Using cluster node discovery with a static seed")

        return ClusterDiscovery(
            StaticSeedFinder([NodeService(discovery_host, discovery_port, None)]),
            retry_policy or DiscoveryRetryPolicy(),
            selector,
            sslcontext=sslcontext,
        )
    except socket.error:
        LOG.info("Using cluster node discovery with DNS")
        resolver = aiodns.DNSResolver()

        return ClusterDiscovery(
            DnsSeedFinder(discovery_host, resolver, discovery_port),
            retry_policy or DiscoveryRetryPolicy(),
            selector,
            sslcontext=sslcontext,
        )
