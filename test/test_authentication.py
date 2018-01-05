from asyncio import Future
from uuid import uuid4

import pytest
from photonpump import messages_pb2 as proto
from photonpump import Header, OperationFlags, Ping, TcpCommand, parse_header


class FakeWriter:

    def __init__(self):
        self.chunks = []

    def write(self, chunk):
        self.chunks.append(chunk)

    def header(self):
        length = self.chunks[0][0:4]
        data = self.chunks[0][4:]

        return parse_header(length, data)

    def body(self, cls):
        result = cls()
        result.ParseFromString(self.chunks[1])

        return result


@pytest.mark.asyncio
async def test_unauthenticated_ping(event_loop):
    correlation_id = uuid4()
    cmd = Ping(correlation_id=correlation_id)

    writer = FakeWriter()
    cmd.send(writer)

    assert writer.header() == Header(
        18, TcpCommand.Ping, OperationFlags.Empty, correlation_id
    )
