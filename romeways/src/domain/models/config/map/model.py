from dataclasses import dataclass
from typing import Type

from romeways.src.core.abstract.infrastructure.queue_connector import AQueueConnector
from romeways.src.domain.models.config.connector import GenericConnectorConfig


@dataclass(slots=True, frozen=True)
class RegionMap:
    spawn_process: bool
    config: GenericConnectorConfig
    connector: Type[AQueueConnector]
