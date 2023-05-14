from dataclasses import dataclass, field


@dataclass(slots=True, frozen=True)
class GenericQueueConfig:
    connector_name: str
    frequency: float
    max_chunk_size: int
    sequential: bool
    resend_on_resolve_fail: bool
