from asyncio import Future
import struct
from uuid import uuid4, UUID

import pytest
from photonpump import messages_pb2 as proto
from photonpump import Header, OperationFlags, Ping, TcpCommand, parse_header, Credential


class FakeWriter:

    def __init__(self):
        self.chunks = []

    def write(self, chunk):
        self.chunks.append(chunk)

    def parse_header(self, length: bytearray, data: bytearray) -> Header:
        (cmd, flags, a, b) = struct.unpack('>BBQQ', data[0:18])
        (size, ) = struct.unpack('<I', length)
        msg_id = UUID(int=(a << 64 | b))

        username = None
        password = None

        if flags == OperationFlags.Authenticated:
            cred = Credential.from_bytes(data[18:])
            username = cred.username
            password = cred.password

        return Header(size, cmd, flags, msg_id, username, password)


    def header(self):
        length = self.chunks[0][0:4]
        data = self.chunks[0][4:]

        return self.parse_header(length, data)

    def body(self, cls):
        result = cls()
        result.ParseFromString(self.chunks[1])

        return result


def test_credential():
    cred = Credential('user', 'pass')
    print(cred.bytes)

    assert len(cred.bytes) == 12
    cred2 = Credential.from_bytes(cred.bytes)

    assert cred2.username == 'user'
    assert cred2.password == 'pass'


@pytest.mark.asyncio
async def test_unauthenticated_ping(event_loop):
    correlation_id = uuid4()
    cmd = Ping(correlation_id=correlation_id)

    writer = FakeWriter()
    cmd.send(writer)

    assert writer.header() == Header(
        18, TcpCommand.Ping, OperationFlags.Empty, correlation_id
    )


@pytest.mark.asyncio
async def test_authenticated_ping(event_loop):
    correlation_id = uuid4()
    credential = Credential('ferret', 'f3rr3t')
    cmd = Ping(
        correlation_id=correlation_id, credential=credential
    )

    writer = FakeWriter()
    cmd.send(writer)

    assert writer.header() == Header(
        34, TcpCommand.Ping, OperationFlags.Authenticated, correlation_id, 'ferret', 'f3rr3t'
    )
