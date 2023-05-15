import asyncio
from unittest.mock import patch

import pytest

from romeways import GenericQueueConfig, GenericConnectorConfig
from romeways.src.domain.models.config.itinerary import Itinerary
from romeways.src.domain.models.config.map import RegionMap
from romeways.src.infrastructure.spawner import Spawner
from romeways.src.service import GuideService
from romeways.src.service.guide.service import guide_service_singleton_ref
from tests.mocs.stubs.queue_connector import StubQueueConnector


def test_register_route():
    guide_service_singleton_ref.clean_references()
    guide_service = GuideService()

    async def callback(message):
        pass

    queue_name = "test_queue_name"
    config = GenericQueueConfig(
        connector_name="test_connector_name",
        frequency=1,
        sequential=False,
        max_chunk_size=10,
    )
    callback = callback

    guide_service.register_route(
        queue_name=queue_name, config=config, callback=callback
    )
    itinerary = Itinerary(
        queue_name=queue_name,
        config=config,
        callback=callback,
    )

    assert guide_service._itineraries[config.connector_name] == [itinerary]


def test_register_connector():
    guide_service_singleton_ref.clean_references()
    guide_service = GuideService()

    connector = StubQueueConnector
    config = GenericConnectorConfig(connector_name="test_connector_name")
    spawn_process = False

    guide_service.register_connector(
        connector=connector, config=config, spawn_process=spawn_process
    )
    region_map = RegionMap(
        spawn_process=spawn_process,
        config=config,
        connector=connector,
    )

    assert guide_service._region_maps[config.connector_name] == region_map


@pytest.mark.asyncio
async def test_start_and_end():
    guide_service_singleton_ref.clean_references()
    guide_service = GuideService()
    guide_service.register_connector(
        connector=StubQueueConnector,
        config=GenericConnectorConfig(connector_name="test_connector_name"),
        spawn_process=False,
    )
    with patch.object(Spawner, "start", return_value=None) as patch_start:
        task = asyncio.create_task(guide_service.start())
        await asyncio.sleep(0)
    with patch.object(Spawner, "close", return_value=None) as patch_close:
        guide_service.end()
    task.cancel()

    patch_start.assert_called_once()
    patch_close.assert_called_once()
