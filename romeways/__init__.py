from .src.romeways import (
    queue_consumer,
    connector_register,
    start,
    GenericConnectorConfig,
    GenericQueueConfig,
    AQueueConnector,
)
from .src.domain.exceptions.exception import ResendException
from .src.domain.models.message import Message

__all__ = [
    "queue_consumer",
    "connector_register",
    "start",
    "GenericConnectorConfig",
    "GenericQueueConfig",
    "AQueueConnector",
    "ResendException",
    "Message",
]

# Memory queue extra
try:
    from romeways_memory_queue import (
        MemoryConnectorConfig,
        MemoryQueueConnector,
        MemoryQueueConfig,
    )

    __all__ += ("MemoryConnectorConfig", "MemoryQueueConnector", "MemoryQueueConfig")
except ImportError as error:  # pragma: no cover
    pass
