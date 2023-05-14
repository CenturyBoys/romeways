from abc import abstractmethod
from typing import List

from romeways.src.core.interfaces.infrastructure.queue_connector import IQueueConnector
from romeways.src.domain.models.config.connector.model import GenericConnectorConfig
from romeways.src.domain.models.config.queue import GenericQueueConfig


class AQueueConnector(IQueueConnector):
    def __init__(
        self,
        connector_config: GenericConnectorConfig,
        queue_name: str,
        config: GenericQueueConfig,
    ):
        self._connector_config = connector_config
        self._queue_name = queue_name
        self._config = config

    @abstractmethod
    async def on_start(self):
        pass

    @abstractmethod
    async def get_messages(self, max_chunk_size: int) -> List[bytes]:
        pass

    @abstractmethod
    async def send_messages(self, message: bytes):
        pass