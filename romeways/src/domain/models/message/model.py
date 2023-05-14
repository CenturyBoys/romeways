import json
from dataclasses import dataclass
from typing import Self


@dataclass
class Message:
    payload: str
    rw_resend_times: int

    @classmethod
    def from_message(cls, message: bytes) -> Self:
        payload = None
        resend_times = 0
        try:
            content = json.loads(message)
            payload = content["payload"]
            if isinstance(payload, dict):
                payload = json.dumps(payload)
            resend_times = content["rw_resend_times"]
        except Exception:
            payload = message.decode()
        finally:
            return cls(payload=payload, rw_resend_times=resend_times)

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__).encode()