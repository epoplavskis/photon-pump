class PhotonPumpException(Exception):
    pass


class StreamNotFoundException(PhotonPumpException):

    def __init__(self, message, stream):
        super().__init__(message)
        self.stream = stream


class BadRequest(PhotonPumpException):

    def __init__(self, conversation_id, message):
        self.conversation = conversation_id
        self.message = message
        super().__init__(conversation_id, message)
