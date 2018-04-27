import asyncio


class FakeProtocol(asyncio.Protocol):

    def __init__(self, name):
        self.name = name
        self.connections_made = 0
        self.data = []
        self.connection_errors = []
        self.expected_received = 0
        self.expected = 0
        self.connected = asyncio.Future()
        self.closed = asyncio.Future()

    def connection_made(self, transport):
        print("%s: connection made" % self.name)
        self.transport = transport
        self.connections_made += 1
        self.connected.set_result(None)

    def data_received(self, data):
        print("%s: data received" % self.name)
        self.data.append(data)
        if self.expectation and not self.expectation.done():
            self.expected_received += len(data)

            if self.expected_received >= self.expected:
                self.expectation.set_result(None)

    def connection_lost(self, exc):
        print("%s: connection lost" % self.name)
        self.closed.set_result(None)
        self.connected = asyncio.Future()
        self.connection_errors.append(exc)

    def expect(self, count):
        self.expected_received = 0
        self.expected = count
        self.expectation = asyncio.Future()
        return self.expectation

    def write(self, msg):
        self.transport.write(msg)
