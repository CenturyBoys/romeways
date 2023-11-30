import asyncio
from asyncio import CancelledError

import pytest

from romeways import AQueueConnector, GenericConnectorConfig
from romeways.src.domain.models.config.map import RegionMap
from romeways.src.infrastructure.spawner import Spawner
from tests.mocs.stubs.queue_connector import StubQueueConnector


@pytest.mark.asyncio
async def test_start_async_worker():
    spawner = Spawner(
        region_map=RegionMap(
            connector=StubQueueConnector,
            config=GenericConnectorConfig(connector_name="tests"),
            spawn_process=False,
        ),
        itineraries=[],
    )

    async def callback_to_spawn(region_map, itineraries):
        while True:
            await asyncio.sleep(1)

    task = asyncio.create_task(spawner.start(callback_to_spawn=callback_to_spawn))
    await asyncio.sleep(0)
    assert len(spawner._tasks) == 1
    spawner.close()
    await asyncio.sleep(0)
    with pytest.raises(CancelledError):
        assert spawner._tasks[0].result()
    task.cancel()


@pytest.mark.asyncio
async def test_start_process_worker():
    spawner = Spawner(
        region_map=RegionMap(
            connector=StubQueueConnector,
            config=GenericConnectorConfig(connector_name="tests"),
            spawn_process=True,
        ),
        itineraries=[],
    )

    async def callback_to_spawn(region_map, itineraries):
        while True:
            await asyncio.sleep(1)

    task = asyncio.create_task(spawner.start(callback_to_spawn=callback_to_spawn))
    await asyncio.sleep(0)
    assert len(spawner._processes) == 1
    spawner.close()
    await asyncio.sleep(0.1)
    assert spawner._processes[0].is_alive() is False
    task.cancel()
