import asyncio
from multiprocessing import Queue

import pytest

from romeways_extras.memory_queue.romeways_memory_queue import (
    MemoryConnectorConfig,
    MemoryQueueConfig,
    MemoryQueueConnector,
)


def get_connector():
    queue = Queue()
    connector_name = "test_connector"
    connector_config = MemoryConnectorConfig(connector_name=connector_name)
    config = MemoryQueueConfig(
        connector_name=connector_name,
        queue=queue,
        frequency=1,
        sequential=False,
        max_chunk_size=10,
    )
    return MemoryQueueConnector(connector_config=connector_config, config=config)


@pytest.mark.asyncio
async def test_get_messages_empty_queue():
    connector = get_connector()
    items = await connector.get_messages(1)
    assert items == []


@pytest.mark.asyncio
async def test_get_messages():
    connector = get_connector()
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
async def test_get_messages_max_chunk_size_eq_0():
    connector = get_connector()
    items = await connector.get_messages(0)
    assert items == []


@pytest.mark.asyncio
async def test_get_messages_max_chunk_size_lt_0():
    connector = get_connector()

    for i in range(1):
        await connector.send_messages(str(i).encode())

    await asyncio.sleep(1)
    items = await connector.get_messages(-1)
    assert items == [b"0"]
