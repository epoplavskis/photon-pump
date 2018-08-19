class PhotonPumpException(Exception):
    pass


class ConversationException(PhotonPumpException):
    def __init__(self, conversation_id, message, *args, **kwargs):
        self.conversation_id = conversation_id
        self.message = message
        super().__init__(conversation_id, message, *args, **kwargs)


class StreamNotFound(ConversationException):
    def __init__(self, conversation_id, stream):
        super().__init__(
            "The stream %s could not be found" % stream, stream, conversation_id
        )
        self.stream = stream
        self.conversation_id = conversation_id


class StreamDeleted(ConversationException):
    def __init__(self, conversation_id, stream):
        super().__init__(
            "The stream %s has been deleted" % stream, stream, conversation_id
        )
        self.stream = stream
        self.conversation_id = conversation_id


class ReadError(ConversationException):
    def __init__(self, conversation_id, stream, error):
        super().__init__(
            "Failed to read from stream %s %s" % (stream, error),
            stream,
            conversation_id,
            error,
        )
        self.stream = stream
        self.conversation_id = conversation_id


class AccessDenied(ConversationException):
    def __init__(self, conversation_id, type, error, **kwargs):
        super().__init__(
            "Access denied for %s %s" % (type, conversation_id),
            conversation_id,
            error,
            kwargs,
        )
        self.conversation_id = conversation_id
        self.conversation_type = type


class EventNotFound(ConversationException):
    def __init__(self, conversation_id, stream, event_number):
        super().__init__(
            "No event %d could be found on stream %s" % (event_number, stream),
            stream,
            conversation_id,
            event_number,
        )
        self.stream = stream
        self.event_number = event_number
        self.conversation_id = conversation_id


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


class SubscriptionCreationFailed(ConversationException):
    pass


class SubscriptionFailed(ConversationException):
    pass


class UnexpectedCommand(ConversationException):
    def __init__(self, expected, actual):
        self.expected = expected
        self.actual = actual

    def __str__(self):
        return "Unexpected command: %s, expected %s" % (self.actual, self.expected)
