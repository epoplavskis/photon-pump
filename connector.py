import asyncio
import logging
import uuid

from photonpump.conversations import Ping, Heartbeat, ConnectPersistentSubscription
from photonpump.connection2 import Connector, MessageReader, MessageWriter, MessageDispatcher
from photonpump.discovery import SingleNodeDiscovery, NodeService, get_discoverer

logging.basicConfig(level=1)

async def connect_yo(loop):
    discovery = get_discoverer(None, None, 'eventstore.service.test.consul', 2113)
    connector = Connector(discovery, loop=loop)


    in_queue = asyncio.Queue(loop=loop)
    out_queue = asyncio.Queue(loop=loop)

    reader = MessageReader(in_queue, connector)
    writer = MessageWriter(out_queue, connector)

    dispatcher = MessageDispatcher(connector, in_queue, out_queue, loop=loop)

    await connector.start()
    dispatcher.start()
    ping = Ping()

    fut = dispatcher.enqueue_conversation(ping)
    await fut

    heartbeat = asyncio.ensure_future(send_heartbeats(dispatcher, connector))

    cmd = ConnectPersistentSubscription(
        'datariver', '$ce-order'
    )
    ce_order_fut = await dispatcher.enqueue_conversation(cmd)
    iterator = await ce_order_fut

    async for e in iterator.events:
        await iterator.ack(e)


async def send_heartbeats(dispatcher, connector):

    while True:
        heartbeat_id = uuid.uuid4()
        logging.info("Sending heartbeat %s to server", heartbeat_id)
        hb = Heartbeat(heartbeat_id, direction=Heartbeat.OUTBOUND)
        fut = await dispatcher.enqueue_conversation(hb)

        try:
            await asyncio.wait_for(fut, 2)
            logging.info("Received heartbeat response from server")
            connector.heartbeat_received(hb.conversation_id)
            await asyncio.sleep(5)
        except asyncio.TimeoutError as e:
            logging.warn("Heartbeat %s timed ou", heartbeat_id)
            connector.heartbeat_failed(e)
        except Exception as exn:
            logging.exception("Heartbeat failed")
            connector.heartbeat_failed(exn)


loop = asyncio.get_event_loop()
asyncio.ensure_future(connect_yo(loop))
loop.run_forever()


