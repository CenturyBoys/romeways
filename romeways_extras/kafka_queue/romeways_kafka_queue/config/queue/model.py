from dataclasses import dataclass

from romeways import GenericQueueConfig


@dataclass(slots=True, frozen=True)
class KafkaQueueConfig(GenericQueueConfig):
    """
    topic: str See aiokafka.AIOKafkaConsumer doc
     -> https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaConsumer
    group_id: str See aiokafka.AIOKafkaConsumer doc.
     -> https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaConsumer
    """
    topic: str
    group_id: str
