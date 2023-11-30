from dataclasses import dataclass

from romeways import GenericConnectorConfig


@dataclass(slots=True, frozen=True)
class KafkaConnectorConfig(GenericConnectorConfig):
    """
    bootstrap_server: str | list[str] See aiokafka.AIOKafkaConsumer doc
     -> https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaConsumer
    client_id: str See aiokafka.AIOKafkaConsumer doc.
     -> https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaConsumer
    """

    bootstrap_server: str | list[str]
    client_id: str
