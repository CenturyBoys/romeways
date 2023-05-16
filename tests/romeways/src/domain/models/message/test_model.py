from romeways import Message


def test_from_message_raw_message_str():
    message = Message.from_message(message=b"a message")
    assert message.rw_resend_times == 0
    assert message.payload == "a message"


def test_from_message_raw_message_int():
    message = Message.from_message(message=b"10")
    assert message.rw_resend_times == 0
    assert message.payload == "10"


def test_from_message_raw_message_json():
    message = Message.from_message(message=b'{"attr": 10}')
    assert message.rw_resend_times == 0
    assert message.payload == '{"attr": 10}'


def test_from_message_raw_message_json_from_resend():
    message = Message.from_message(
        message=b'{"payload": {"attr": 10}, "rw_resend_times":1}'
    )
    assert message.rw_resend_times == 1
    assert message.payload == '{"attr": 10}'


def test_to_json():
    message = Message.from_message(message=b"10")
    assert message.rw_resend_times == 0
    assert message.payload == "10"
    json_message = message.to_json()
    assert json_message == b'{"payload": "10", "rw_resend_times": 0}'


def test_rebuild_message_from_message_to_json():
    message_a = Message.from_message(message=b"10")
    json_message = message_a.to_json()
    message_b = Message.from_message(message=json_message)
    assert message_b == message_a
