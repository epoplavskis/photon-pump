import uuid
from photonpump.conversations import Ping, Heartbeat


def test_conversation_equality_is_based_on_id():

    conversation_id_1 = uuid.uuid4()
    conversation_id_2 = uuid.uuid4()

    ping_a = Ping(conversation_id_1)
    ping_b = Ping(conversation_id_1)
    ping_c = Ping(conversation_id_2)

    assert ping_a == ping_b
    assert ping_b != ping_c
    assert ping_c != ping_a


def test_conversation_str_contains_the_type_and_id():

    """
    TODO: add tests for another couple of conversation types
    """
    conversation_id = uuid.UUID("1ad12fdc-ebf0-4d07-8bfd-e8917ac7aa74")
    ping = Ping(conversation_id)

    assert str(ping) == "<Ping 1ad12fdc-ebf0-4d07-8bfd-e8917ac7aa74>"
