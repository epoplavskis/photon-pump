class PhotonPumpException(Exception):
    pass


class StreamNotFoundException(PhotonPumpException):

    def __init__(self, message, stream):
        super().__init__(message)
        self.stream = stream


class ConversationException(PhotonPumpException):

    def __init__(self, conversation_id, message, *args):
        self.conversation_id = conversation_id
        self.message = message
        super().__init__(conversation_id, message, *args)


class BadRequest(ConversationException):
    pass


class NotAuthenticated(ConversationException):
    pass


class MessageUnhandled(ConversationException):

    def __init__(self, conversation_id, message, reason):
        super().__init__(conversation_id, message)
        self.reason = reason


class NotReady(ConversationException):

    def __init__(self, conversation_id):
        super().__init__(conversation_id, "Message not handled: Server not ready")


class TooBusy(ConversationException):

    def __init__(self, conversation_id):
        super().__init__(conversation_id, "Message not handled: Server too busy")


class NotMaster(ConversationException):

    def __init__(self, conversation_id):
        super().__init__(conversation_id, "Message not handled: Must be sent to master")


class NotHandled(ConversationException):

    def __init__(self, conversation_id, reason):
        super().__init__(conversation_id, "Message not handled: Unknown reason code.")


class PayloadUnreadable(ConversationException):

    def __init__(self, conversation_id, payload, exn):
        self.payload = payload
        super().__init__(conversation_id, "The response could not be read", exn) 


