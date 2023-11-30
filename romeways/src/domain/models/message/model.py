import json
from dataclasses import dataclass
from json import JSONDecodeError
from typing import Self


@dataclass(slots=True)
class Message:
    payload: str
    rw_resend_times: int

    @classmethod
    def from_message(cls, message: bytes) -> Self:
        resend_times = 0
        try:
            content = json.loads(message)
            resend_times = content["rw_resend_times"]
            payload = content["payload"]
            if isinstance(payload, dict):
                payload = json.dumps(payload)
        except (JSONDecodeError, KeyError, TypeError):
            payload = message.decode()

        return cls(payload=payload, rw_resend_times=resend_times)

    def raise_resend_counter(self) -> Self:
        return Message(self.payload, self.rw_resend_times + 1)

    def to_json(self):
        return json.dumps(
            {
                "payload": self.payload,
                "rw_resend_times": self.rw_resend_times,
            }
        ).encode()
