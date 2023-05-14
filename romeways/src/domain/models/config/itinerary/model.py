from dataclasses import dataclass
from typing import Callable

from romeways.src.domain.models.config.queue import GenericQueueConfig


@dataclass(slots=True, frozen=True)
class Itinerary:
    queue_name: str
    config: GenericQueueConfig
    callback: Callable
