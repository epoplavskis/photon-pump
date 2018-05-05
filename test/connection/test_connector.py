"""
This module tests the Connector. He's a cool guy. He tries to connect to
Eventstore and doesn't quit until he's exhausted every option. He only deals
with TCP connections, StreamReaders, and StreamWriters, so we can test him with
a simple echo server.

When he makes a connection, loses a connection, or quits trying, he raises an
event. These events are handled by the Reader, Writer, and Dispatcher.
"""
