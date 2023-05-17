from dataclasses import dataclass
from multiprocessing import Queue

from romeways import GenericQueueConfig


@dataclass(slots=True, frozen=True)
class MemoryQueueConfig(GenericQueueConfig):
    queue: Queue
