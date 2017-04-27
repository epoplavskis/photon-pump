import asyncio
from asyncio import Future, ensure_future
from collections import namedtuple
import struct
import uuid

Header = namedtuple('photonpump_result_header', ['size', 'cmd', 'flags', 'correlation_id'])
Pong = namedtuple('photonpump_result_Pong', ['correlation_id'])

#: 1 byte command + 1 byte auth + UUID correlation length
HEADER_LENGTH = 1 + 1 + 16
FLAGS_NONE = 0x00

class TcpCommand:
    Ping = 0x03
    Pong = 0x04


result_readers = {}
result_readers[TcpCommand.Pong] = lambda head, _: Pong(head.correlation_id)

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
           (cmd, flags,) = struct.unpack_from('bb', next_msg, 0)
           id = uuid.UUID(bytes=next_msg[2:HEADER_LENGTH])
           header = Header(size, cmd, flags, id)
           result = result_readers[cmd](header, None)
           self._futures[id].set_result(result)
           del self._futures[id]

    def close(self):
        self.writer.close()
        self.read_loop.cancel()
        self.disconnected()

    async def ping(self, correlation_id=uuid.uuid4()):
       print(correlation_id)
       fut = Future(loop=self.loop)
       self._futures[correlation_id] = fut
       buf = bytearray()
       buf.extend(struct.pack('<I', HEADER_LENGTH))
       buf.append(TcpCommand.Ping)
       buf.append(FLAGS_NONE)
       buf.extend(correlation_id.bytes)
       self.writer.write(buf)
       return await fut
