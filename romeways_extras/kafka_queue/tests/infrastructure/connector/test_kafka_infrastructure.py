import asyncio
from multiprocessing import Queue
from unittest.mock import patch, AsyncMock

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
from kafka import TopicPartition

from romeways import (
    KafkaConnectorConfig,
    KafkaQueueConfig,
    KafkaQueueConnector,
)


class QueueStubManager:
    _queues = {}

    @classmethod
    def get_queue(cls, topic: str, bootstrap_servers: str):
        _hash = f"{topic}{bootstrap_servers}"
        if _hash not in cls._queues:
            cls._queues[_hash] = Queue()
        return cls._queues[_hash]


class StubAIOKafkaConsumer:
    def __init__(self, *args, bootstrap_server: str, client_id: str, group_id: str):
        self._bootstrap_server = bootstrap_server
        self._topic = args[0]
        self._partition = 0
        self._offset = 0
        self._client_id = client_id
        self._group_id = group_id

    async def getmany(self, max_records: int):
        queue = QueueStubManager.get_queue(
            topic=self._topic, bootstrap_servers=self._bootstrap_server
        )

        buffer = []
        for _ in range(abs(max_records)):
            if not queue.empty():
                self._offset += 1
                buffer.append(
                    ConsumerRecord(
                        topic=self._topic,
                        partition=self._partition,
                        offset=self._offset,
                        timestamp=0,
                        timestamp_type=1,
                        key=None,
                        value=queue.get_nowait(),
                        checksum=0,
                        serialized_key_size=0,
                        serialized_value_size=0,
                        headers=None,
                    )
                )
                continue
            break
        return {TopicPartition(self._topic, self._partition): buffer}

    async def start(self):
        pass


class StubAIOKafkaProducer:
    def __init__(self, bootstrap_server: str):
        self._bootstrap_server = bootstrap_server

    async def send(self, topic: str, message: bytes):
        queue = QueueStubManager.get_queue(
            topic=topic, bootstrap_servers=self._bootstrap_server
        )
        queue.put_nowait(message)

    async def start(self):
        pass


@pytest.mark.asyncio
async def test_on_start():
    connector_name = "tests"
    connector_config = KafkaConnectorConfig(
        connector_name=connector_name,
        bootstrap_server="localhost:9200",
        client_id="romeways_client_id",
    )
    config = KafkaQueueConfig(
        connector_name=connector_name,
        topic="test_topic",
        group_id="romeways_group_id",
        frequency=1,
        sequential=False,
        max_chunk_size=10,
    )
    connector = KafkaQueueConnector(connector_config=connector_config, config=config)
    with patch.object(
        AIOKafkaConsumer, "__new__", return_value=AsyncMock()
    ) as consumer:
        with patch.object(
            AIOKafkaProducer, "__new__", return_value=AsyncMock()
        ) as producer:
            await connector.on_start()
    consumer.assert_called_once_with(
        AIOKafkaConsumer,
        config.topic,
        bootstrap_servers=connector_config.bootstrap_server,
        client_id=connector_config.client_id,
        group_id=config.group_id,
    )
    producer.assert_called_once_with(
        AIOKafkaProducer, bootstrap_servers=connector_config.bootstrap_server
    )


async def get_connector():
    connector_name = "tests"
    bootstrap_server = "localhost:9200"
    client_id = "romeways_client_id"
    group_id = "romeways_group_id"
    topic = "test_topic"
    connector_config = KafkaConnectorConfig(
        connector_name=connector_name,
        bootstrap_server=bootstrap_server,
        client_id=client_id,
    )
    config = KafkaQueueConfig(
        connector_name=connector_name,
        topic=topic,
        group_id=group_id,
        frequency=1,
        sequential=False,
        max_chunk_size=10,
    )
    connector = KafkaQueueConnector(connector_config=connector_config, config=config)
    connector._producer = StubAIOKafkaProducer(  # pylint: disable=W0212
        bootstrap_server=bootstrap_server
    )
    connector._consumer = StubAIOKafkaConsumer(  # pylint: disable=W0212
        topic,
        bootstrap_server=bootstrap_server,
        client_id=client_id,
        group_id=group_id,
    )
    return connector


@pytest.mark.asyncio
async def test_get_messages_empty_queue():
    connector = await get_connector()
    items = await connector.get_messages(1)
    assert items == []


@pytest.mark.asyncio
async def test_get_messages():
    connector = await get_connector()
    for i in range(5):
        await connector.send_messages(str(i).encode())
    await asyncio.sleep(1)
    items_a = await connector.get_messages(2)
    items_b = await connector.get_messages(2)
    items_c = await connector.get_messages(2)
    assert items_a == [b"0", b"1"]
    assert items_b == [b"2", b"3"]
    assert items_c == [b"4"]


@pytest.mark.asyncio
async def test_get_messages_without_none():
    connector = await get_connector()
    for i in [str(0).encode(), str(1).encode(), None, str(3).encode()]:
        await connector.send_messages(i)
    await asyncio.sleep(1)
    items_a = await connector.get_messages(2)
    items_b = await connector.get_messages(2)
    assert items_a == [b"0", b"1"]
    assert items_b == [b"3"]


@pytest.mark.asyncio
async def test_get_messages_max_chunk_size_eq_0():
    connector = await get_connector()
    items = await connector.get_messages(0)
    assert items == []


@pytest.mark.asyncio
async def test_get_messages_max_chunk_size_lt_0():
    connector = await get_connector()

    for i in range(1):
        await connector.send_messages(str(i).encode())

    await asyncio.sleep(1)
    items = await connector.get_messages(-1)
    assert items == [b"0"]
