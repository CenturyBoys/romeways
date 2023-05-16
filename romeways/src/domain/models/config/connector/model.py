from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class GenericConnectorConfig:
    """
    connector_name: str Connector name
    """

    connector_name: str
