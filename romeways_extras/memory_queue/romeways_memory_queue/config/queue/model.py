from dataclasses import dataclass, field

from romeways import GenericQueueConfig


@dataclass(slots=True, frozen=True)
class MemoryQueueConfig(GenericQueueConfig):
    connector_name: str
    frequency: float = field(default=0.25)
    max_chunk_size: int = field(default=10)
    sequential: bool = field(default=False)
    resend_on_resolve_fail: bool = field(default=True)
