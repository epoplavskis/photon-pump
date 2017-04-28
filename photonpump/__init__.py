import asyncio
from asyncio import Future, ensure_future
from collections import namedtuple
import io
import json
import struct
from typing import Dict, List, Any
import uuid
from .messages import TcpCommand, Pong, NewEvent, WriteEvents, Header, Ping
from . import messages_pb2


HEADER_LENGTH = 1 + 1 + 16

#: 1 byte command + 1 byte auth + UUID correlation length
FLAGS_NONE = 0x00

def read_writecompleted(header, payload):
    result = messages_pb2.WriteEventsCompleted()
    result.ParseFromString(payload)
    return result


result_readers = {}
result_readers[TcpCommand.Pong] = lambda head, _: Pong(head.correlation_id)
result_readers[TcpCommand.WriteEventsCompleted] = read_writecompleted

class Event(list):

    def __call__(self, *args, **kwargs):
        for f in self:
            f(*args, **kwargs)

    def __repr__(self):
        return "Event(%)" % list.__repr__(self)

class OutChannel:

    def __init__(self, writer, loop=None):
        self.queue = asyncio.Queue(maxize=100, loop=loop)
        self.writer = writer

    async def enqueue(self, message):
        await self.queue.put(message)

    async def _process(self):
        while True:
            next = await self.queue.get()
            self.write_header(next.header)
            self.writer.write(next.payload)



class Connection:

    def __init__(self, host='127.0.0.1', port=1113, loop=None):
        self.connected = Event()
        self.disconnected = Event()
        self.host = host
        self.port = port
        self.loop = loop
        self._futures = {}

    async def connect(self):
        reader, writer = await asyncio.open_connection(self.host, self.port, loop=self.loop)
        self.writer = writer
        self.read_loop = asyncio.ensure_future(self._read_responses(reader))
        self.connected()

    async def _read_responses(self, reader):
       while True:
           next_msg_len = await reader.read(4)
           (size,) = struct.unpack('I', next_msg_len)
           next_msg = await reader.read(size)
           (cmd, flags,) = struct.unpack_from('BB', next_msg, 0)
           id = uuid.UUID(bytes=next_msg[2:HEADER_LENGTH])
           print(id)
           header = Header(size, cmd, flags, id)
           try:
               result = result_readers[cmd](header, next_msg[HEADER_LENGTH:])
               self._futures[id].set_result(result)
           except Exception as e:
               self._futures[id].set_exception(e)
           del self._futures[id]

    def close(self):
        self.writer.close()
        self.read_loop.cancel()
        self.disconnected()

    async def ping(self, correlation_id=uuid.uuid4()):
        cmd = Ping(correlation_id=correlation_id, loop=self.loop)
        self._futures[cmd.correlation_id] = cmd.future
        cmd.send(self.writer)
        return await cmd.future

    async def publish_event(self, type, stream, body=None, id=uuid.uuid4(), metadata=None, expected_version=-2, require_master=False):

       event = NewEvent(type, id, body, metadata)
       cmd = WriteEvents(stream, [event], expected_version=expected_version, require_master=require_master, loop=self.loop)
       self._futures[cmd.correlation_id] = cmd.future
       cmd.send(self.writer)
       return await cmd.future


class ConnectionContextManager:

    def __init__(self, host='127.0.0.1', port=1113, loop=None):
        self.conn = Connection(host=host, port=port, loop=loop)

    async def __aenter__(self):
        await self.conn.connect()
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        self.conn.close()


def connect(*args, **kwargs):
    return ConnectionContextManager(*args, **kwargs)
