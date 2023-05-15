from typing import List

from romeways import AQueueConnector


class StubQueueConnector(AQueueConnector):
    async def on_start(self):
        pass

    async def get_messages(self, max_chunk_size: int) -> List[bytes]:
        pass

    async def send_messages(self, message: bytes):
        pass
