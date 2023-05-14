from dataclasses import dataclass
from multiprocessing import Queue

from romeways import GenericConnectorConfig


@dataclass(slots=True, frozen=True)
class MemoryConnectorConfig(GenericConnectorConfig):
    queue: Queue
