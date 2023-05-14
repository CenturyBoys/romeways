from abc import abstractmethod, ABC
from typing import Callable, Type

from romeways.src.core.abstract.infrastructure.queue_connector import AQueueConnector
from romeways.src.domain.models.config.connector import GenericConnectorConfig
from romeways.src.domain.models.config.queue import GenericQueueConfig


class IGuide(ABC):
    @abstractmethod
    def register_route(
        self, queue_name: str, config: GenericQueueConfig, callback: Callable
    ):
        pass

    @abstractmethod
    def register_connector(
        self,
        connector: Type[AQueueConnector],
        config: GenericConnectorConfig,
        spawn_process: bool,
    ):
        pass

    @abstractmethod
    async def start(self):
        pass

    @abstractmethod
    def end(self):
        pass
