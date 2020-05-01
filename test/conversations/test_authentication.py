from uuid import uuid4

import pytest

from photonpump.conversations import Ping
from photonpump.messages import Credential

from ..fakes import TeeQueue


@pytest.mark.asyncio
async def test_authenticated_request():

    output = TeeQueue()

    conversation_id = uuid4()
    credential = Credential("username", "password")
    convo = Ping(conversation_id, credential)

    await convo.start(output)

    request = await output.get()

    assert request.header_bytes == b"".join(
        [
            b"\x24\x00\x00\x00" b"\x03",  # 36 == header_size  # TcpCommand.Ping
            b"\x01",  # Authentication bit is set
            conversation_id.bytes_le,  # Conversation_id
            b"\x08",  # 8 == len(username)
            b"username",
            b"\x08",  # 8 == len(password)
            b"password",
        ]
    )
