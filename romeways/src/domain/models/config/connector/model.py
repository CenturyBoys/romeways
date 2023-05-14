from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class GenericConnectorConfig:
    connector_name: str