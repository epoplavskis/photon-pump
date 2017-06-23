class PhotonPumpException(Exception):
    pass


class StreamNotFoundException(PhotonPumpException):

    def __init__(self, message, stream):
        super().__init__(message)
        self.stream = stream
