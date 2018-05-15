import asyncio
import uuid

import pytest

from photonpump import TcpCommand
from photonpump import messages as msg
from photonpump.connection import Connector, MessageReader, MessageWriter
from photonpump.discovery import NodeService, SingleNodeDiscovery

from .test_connector import EchoServer


@pytest.mark.asyncio
async def test_write_message(event_loop):

    addr = NodeService("localhost", 8338, None)
    async with EchoServer(addr, event_loop):

        connector = Connector(SingleNodeDiscovery(addr), loop=event_loop)

        output_queue = asyncio.Queue(maxsize=100)
        writer = MessageWriter(output_queue, connector)

        input_queue = asyncio.Queue(maxsize=100)
        _ = MessageReader(input_queue, connector)

        wait_for = asyncio.Future(loop=event_loop)

        def on_connected(reader, writer):
            print("Called!")
            wait_for.set_result(None)

        connector.connected.append(on_connected)
        await connector.start()

        # connector timeout
        await asyncio.wait_for(wait_for, 2)

        outbound_message = msg.OutboundMessage(
            uuid.uuid4(), TcpCommand.HeartbeatResponse, bytes()
        )
        await writer.enqueue_message(outbound_message)

        inbound_message = await asyncio.wait_for(input_queue.get(), 5)

        assert inbound_message.payload == outbound_message.payload

        await writer.close()
        # await reader.close()
        await connector.stop()
