from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class GenericQueueConfig:
    connector_name: str
    frequency: float
    max_chunk_size: int
    sequential: bool
