import logging
from typing import List

from aiokafka import AIOKafkaConsumer, ConsumerRecord, AIOKafkaProducer
from kafka import TopicPartition

from romeways import AQueueConnector

from romeways_kafka_queue import KafkaQueueConfig
from romeways_kafka_queue.config import KafkaConnectorConfig


class KafkaQueueConnector(AQueueConnector):
    _connector_config: KafkaConnectorConfig
    _config: KafkaQueueConfig
    _consumer: AIOKafkaConsumer
    _producer: AIOKafkaProducer

    async def on_start(self):
        self._consumer = AIOKafkaConsumer(
            self._config.topic,
            bootstrap_servers=self._connector_config.bootstrap_server,
            client_id=self._connector_config.client_id,
            group_id=self._config.group_id,
        )
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._connector_config.bootstrap_server
        )
        await self._producer.start()
        await self._consumer.start()

    async def get_messages(self, max_chunk_size: int) -> List[bytes]:
        data: dict[TopicPartition, list[ConsumerRecord]] = await self._consumer.getmany(
            max_records=abs(max_chunk_size)
        )
        buffer = []
        for messages in data.values():
            for msg in messages:
                if msg.value is None:
                    logging.warning(
                        "Message from topic '%s' with partition '%s', offset '%d', "
                        "key '%s' and timestamp '%d' without payload. Discard message.",
                        msg.topic,
                        msg.partition,
                        msg.offset,
                        msg.key,
                        msg.timestamp,
                    )
                    continue
                buffer.append(msg.value)
        return buffer

    async def send_messages(self, message: bytes):
        await self._producer.send(self._config.topic, message)
