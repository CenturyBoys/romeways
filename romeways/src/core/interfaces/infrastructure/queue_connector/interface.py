from abc import ABC, abstractmethod
from typing import List

from romeways.src.domain.models.config.connector import GenericConnectorConfig
from romeways.src.domain.models.config.queue import GenericQueueConfig


class IQueueConnector(ABC):
    connector_config: GenericConnectorConfig
    queue_name: str
    config: GenericQueueConfig

    @abstractmethod
    async def on_start(self):
        pass

    @abstractmethod
    async def get_messages(self, max_chunk_size: int) -> List[bytes]:
        pass

    @abstractmethod
    async def send_messages(self, message: bytes):
        pass
