from multiprocessing import Queue
from typing import List

from romeways import AQueueConnector


class MemoryQueueConnector(AQueueConnector):
    async def on_start(self):
        pass

    async def get_messages(self, max_chunk_size: int) -> List[bytes]:
        queue: Queue = self._connector_config.queue
        buffer = []
        for _ in range(abs(max_chunk_size)):
            if not queue.empty():
                buffer.append(queue.get_nowait())
                continue
            break
        return buffer

    async def send_messages(self, message: bytes):
        queue: Queue = self._connector_config.queue
        queue.put_nowait(message)
