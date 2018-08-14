import asyncio
import binascii
import uuid

import pytest

from photonpump import messages_pb2 as proto
from photonpump.connection import MessageReader
from photonpump.messages import TcpCommand
from .fakes import TeeQueue


def read_hex(s):
    return binascii.unhexlify("".join(s.split()))


heartbeat_data = read_hex(
    "12 00 00 00 01 00 9f 65 81 c1 0b 80 58 4b a8 5d 5f d3 fd c5 23 B9"
)

heartbeat_id = uuid.UUID("c181659f-800b-4b58-a85d-5fd3fdc523b9")

persistent_stream_event_appeared = read_hex(
    """
ef 01 00 00
c7 00 2f d7 92 f1 bd 7a e4 4a ae 05 f2 06 87 3c
74 9d
0a da 03 0a c4 01 0a 31 43 61 6e 63 65 6c 6c 61
74 69 6f 6e 2d 64 37 36 32 61 35 33 34 2d 36 62
63 36 2d 34 37 36 38 2d 61 66 32 38 2d 32 61 62
63 66 37 31 61 31 33 34 39 10 00 1a 10 50 36 07
1d de 79 4f 35 88 25 c1 a7 ae 81 0b a8 22 14 63
61 6e 63 65 6c 6c 61 74 69 6f 6e 5f 73 74 61 72
74 65 64 28 01 30 01 3a 4e 7b 22 6f 72 64 65 72
5f 69 64 22 3a 20 22 31 32 33 22 2c 20 22 63 61
6e 63 65 6c 6c 61 74 69 6f 6e 5f 69 64 22 3a 20
22 64 37 36 32 61 35 33 34 2d 36 62 63 36 2d 34
37 36 38 2d 61 66 32 38 2d 32 61 62 63 66 37 31
61 31 33 34 39 22 7d 42 00 48 f8 cb b4 d7 e8 ec
d5 ea 08 50 b3 ab 9a d9 8d 2c 12 90 02 0a 10 24
63 65 2d 43 61 6e 63 65 6c 6c 61 74 69 6f 6e 10
5d 1a 10 cc 6c 11 34 33 d4 93 4b bb d1 1c 79 d9
10 16 75 22 02 24 3e 28 00 30 00 3a 33 30 40 43
61 6e 63 65 6c 6c 61 74 69 6f 6e 2d 64 37 36 32
61 35 33 34 2d 36 62 63 36 2d 34 37 36 38 2d 61
66 32 38 2d 32 61 62 63 66 37 31 61 31 33 34 39
42 99 01 7b 22 24 76 22 3a 22 31 3a 2d 31 3a 31
3a 33 22 2c 22 24 63 22 3a 33 34 34 30 37 36 31
30 2c 22 24 70 22 3a 33 34 34 30 37 36 31 30 2c
22 24 6f 22 3a 22 43 61 6e 63 65 6c 6c 61 74 69
6f 6e 2d 64 37 36 32 61 35 33 34 2d 36 62 63 36
2d 34 37 36 38 2d 61 66 32 38 2d 32 61 62 63 66
37 31 61 31 33 34 39 22 2c 22 24 63 61 75 73 65
64 42 79 22 3a 22 31 64 30 37 33 36 35 30 2d 37
39 64 65 2d 33 35 34 66 2d 38 38 32 35 2d 63 31
61 37 61 65 38 31 30 62 61 38 22 7d 48 d2 e3 c7
d7 e8 ec d5 ea 08 50 d2 ab 9a d9 8d 2c
"""
)

ReadEventResult = read_hex(
    """
9c 00 00 00 b1 00 f3 b9 4a 36 6c fe 6d 43 a3 65
be ad 2e 1c e3 6b 08 00 12 85 01 0a 82 01 0a 24
64 37 39 32 34 64 37 35 2d 38 32 62 66 2d 34 37
36 34 2d 39 35 39 64 2d 31 30 36 31 32 62 61 32
39 38 37 33 10 00 1a 10 7d 27 65 7b 68 16 45 44
bf 1a 5d eb 90 eb dc d6 22 0e 74 68 69 6e 67 5f
68 61 70 70 65 6e 65 64 28 01 30 01 3a 1f 7b 22
74 68 69 6e 67 22 3a 20 31 2c 20 22 68 61 70 70
65 6e 69 6e 67 22 3a 20 74 72 75 65 7d 42 00 48
fc 9d cd d8 94 b9 d6 ea 08 50 a4 e6 a4 d6 8e 2c
"""
)


class message_reader:
    async def __aenter__(self):
        messages = TeeQueue()
        stream = asyncio.StreamReader()
        reader = MessageReader(stream, 1, messages)
        self.read_loop = asyncio.ensure_future(reader.start())
        return reader, messages

    async def __aexit__(self, *args, **kwargs):
        self.read_loop.cancel()
        try:
            await self.read_loop
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_read_event():
    async with message_reader() as (stream, messages):

        stream.feed_data(ReadEventResult)

        received = await messages.get()

        assert received.command == TcpCommand.ReadEventCompleted
        assert received.length == 156

        body = proto.ReadEventCompleted()
        body.ParseFromString(received.payload)

        event = body.event.event
        assert event.event_number == 0
        assert event.event_type == "thing_happened"


@pytest.mark.asyncio
async def test_read_heartbeat_request_single_call():

    async with message_reader() as (stream, messages):
        stream.feed_data(heartbeat_data)

        received = await messages.get()

        assert received.payload == b""
        assert received.command == TcpCommand.HeartbeatRequest
        assert received.length == 18
        assert received.conversation_id == heartbeat_id


@pytest.mark.asyncio
async def test_read_header_multiple_calls():
    async with message_reader() as (stream, messages):
        stream.feed_data(heartbeat_data[0:2])
        stream.feed_data(heartbeat_data[2:7])
        stream.feed_data([])
        stream.feed_data(heartbeat_data[7:14])
        stream.feed_data(heartbeat_data[14:])

        received = await messages.get()

        assert received.payload == b""
        assert received.command == TcpCommand.HeartbeatRequest
        assert received.length == 18
        assert received.conversation_id == heartbeat_id


@pytest.mark.asyncio
async def test_a_message_with_a_payload():
    async with message_reader() as (stream, messages):

        stream.feed_data(persistent_stream_event_appeared)

        received = await messages.get()
        assert received.conversation_id == uuid.UUID(
            "f192d72f-7abd-4ae4-ae05-f206873c749d"
        )
        assert received.command == TcpCommand.PersistentSubscriptionStreamEventAppeared


@pytest.mark.asyncio
async def test_two_messages_one_call():

    async with message_reader() as (stream, messages):
        stream.feed_data(heartbeat_data + persistent_stream_event_appeared)

        heartbeat = await messages.get()
        event = await messages.get()

        assert heartbeat.conversation_id == heartbeat_id
        assert event.conversation_id == uuid.UUID(
            "f192d72f-7abd-4ae4-ae05-f206873c749d"
        )


@pytest.mark.asyncio
async def test_three_messages_two_calls():

    data = heartbeat_data + persistent_stream_event_appeared + heartbeat_data

    async with message_reader() as (stream, messages):

        stream.feed_data(data[0:250])
        await messages.next_event()
        assert len(messages.items) == 1

        stream.feed_data(data[250:])
        await messages.next_event(count=2)

        [first_heartbeat, event, second_heartbeat] = messages.items

        assert (
            first_heartbeat.conversation_id
            == heartbeat_id
            == second_heartbeat.conversation_id
        )
        assert event.conversation_id == uuid.UUID(
            "f192d72f-7abd-4ae4-ae05-f206873c749d"
        )


@pytest.mark.asyncio
async def test_two_messages_three_calls():

    data = heartbeat_data + persistent_stream_event_appeared

    async with message_reader() as (stream, messages):

        stream.feed_data(data[0:125])
        stream.feed_data(data[125:])
        heartbeat = await messages.get()
        event = await messages.get()

        assert heartbeat.conversation_id == heartbeat_id
        assert event.conversation_id == uuid.UUID(
            "f192d72f-7abd-4ae4-ae05-f206873c749d"
        )

        assert len(messages.items) == 2
