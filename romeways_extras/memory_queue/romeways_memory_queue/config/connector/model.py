from dataclasses import dataclass

from romeways import GenericConnectorConfig


@dataclass(slots=True, frozen=True)
class MemoryConnectorConfig(GenericConnectorConfig):
    pass
