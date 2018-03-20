#!/usr/bin/env python

import asyncio
import logging

import photonpump
from photonpump.exceptions import SubscriptionCreationFailed



logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger('cancellation.event-reader')


async def heartbeat_actor(subscription):
    async for event in subscription.events:
        try:
            LOG.info('hearbeat processed', exc_info=True)
        except Exception:
            LOG.error('failed to process heartbeat', exc_info=True)
        finally:
            await subscription.ack(event)


async def setup_subscriptions(conn):
    heartbeat_subscription = 'stufff'
    heartbeat_stream = 'heartbeat'

    try:
        await conn.create_subscription(
            heartbeat_subscription,
            heartbeat_stream,
            start_from=-1
        )
    except SubscriptionCreationFailed as exn:
        LOG.warn(exn)

    return await asyncio.gather(
        conn.connect_subscription(
            heartbeat_subscription,
            heartbeat_stream
        )
    )



async def run(cfg):
    async with photonpump.connect(host=cfg['host'],
                                  port=cfg['port'],
                                  username=cfg['username'],
                                  password=cfg['password']) as conn:

        try:
            # await asyncio.sleep(200)
            [heartbeat] = await setup_subscriptions(conn)
            LOG.info(
                "Created subscriptions %s", heartbeat
            )
        except Exception as exn:
            LOG.error(
                "Failed while connecting to subscriptions: %s \n sleeping before exit",
                exn,
                exc_info=True
            )

            return

        return await asyncio.gather(
            heartbeat_actor(heartbeat)
        )


cfg = {
    'host':'localhost',
    'port': '1113',
    'username': 'admin',
    'password': 'changeit',
}

loop = asyncio.get_event_loop()
loop.run_until_complete(run(cfg))
