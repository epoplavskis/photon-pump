import asyncio
from asyncio import Future, ensure_future
from collections import namedtuple
import io
import json
import struct
import uuid
from . import messages_pb2 as messages

Header = namedtuple('photonpump_result_header', ['size', 'cmd', 'flags', 'correlation_id'])
Pong = namedtuple('photonpump_result_Pong', ['correlation_id'])
EventData = namedtuple('photonpump_event', ['eventId', 'eventType', 'data'])

#: 1 byte command + 1 byte auth + UUID correlation length
HEADER_LENGTH = 1 + 1 + 16
FLAGS_NONE = 0x00

class TcpCommand:
    Ping = 0x03
    Pong = 0x04

    WriteEvents = 0x82
    WriteEventsCompleted = 0x83

def read_writecompleted(header, payload):
    result = messages.WriteEventsCompleted()
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
        fut = Future(loop=self.loop)
        self._futures[correlation_id] = fut
        buf = bytearray()
        self.write_header(buf, 0, TcpCommand.Ping, FLAGS_NONE, correlation_id)
        self.writer.write(buf)
        return await fut

    async def publish_event(self, event_type, stream, body=None, eventId=uuid.uuid4(), expected_version=-2, require_master=False):
       buf = bytearray()

       msg = messages_pb2.WriteEvents()
       msg.event_stream_id = stream
       msg.require_master = require_master
       msg.expected_version = expected_version

       event = msg.events.add()
       event.event_id = eventId.bytes
       event.event_type = event_type
       event.data_content_type = 1
       event.data = json.dumps(body).encode('UTF-8')
       event.metadata_content_type = 0
       serialized = msg.SerializeToString()

       fut = Future(loop=self.loop)
       correlation_id = uuid.uuid4()
       self._futures[correlation_id] = fut

       print(correlation_id)
       self.write_header(buf, len(serialized), TcpCommand.WriteEvents, FLAGS_NONE, correlation_id)
       self.writer.write(buf)
       self.writer.write(serialized)

       return await fut

    def write_header(self, buf, data_length, cmd, flags, correlation_id):
        buf.extend(struct.pack('<I', HEADER_LENGTH + data_length))
        buf.append(cmd)
        buf.append(flags)
        buf.extend(correlation_id.bytes)

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
