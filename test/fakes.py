from photonpump.connection import Event


class FakeConnector():

    def __init__(self,):
        self.connected = Event()
        self.disconnected = Event()
        self.stopped = Event()
