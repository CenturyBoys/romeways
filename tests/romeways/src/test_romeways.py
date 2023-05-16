from unittest.mock import patch

import pytest

import romeways
from romeways import GenericQueueConfig, GenericConnectorConfig
from romeways.src.service.guide import GuideService
from romeways.src.service.guide.service import guide_service_singleton_ref
from tests.mocs.stubs.queue_connector import StubQueueConnector


def test_queue_consumer_wrong_config():
    class StubConfig:
        pass

    with pytest.raises(TypeError) as exception:

        @romeways.queue_consumer(queue_name="test_queue_name", config=StubConfig())
        async def controller():
            pass

    assert (
        exception.value.args[0]
        == "The config attribute is not a subclass of GenericQueueConfig"
    )


def test_queue_consumer_invalid_controller():

    config = GenericQueueConfig(
        connector_name="test_connector_name",
        max_chunk_size=10,
        frequency=1,
        sequential=False,
    )

    with pytest.raises(TypeError) as exception:

        @romeways.queue_consumer(queue_name="test_queue_name", config=config)
        def controller():
            pass

    assert exception.value.args[0] == "The callback is not a coroutine function"


def test_queue_consumer():

    guide_service_singleton_ref.clean_references()

    config = GenericQueueConfig(
        connector_name="test_connector_name",
        max_chunk_size=10,
        frequency=1,
        sequential=False,
    )

    @romeways.queue_consumer(queue_name="test_queue_name", config=config)
    async def controller(message):
        pass


def test_connector_register_wrong_config_type():
    class StubConfig:
        pass

    class StubConnector:
        pass

    with pytest.raises(TypeError) as exception:
        romeways.connector_register(
            connector=StubConnector, config=StubConfig(), spawn_process=False
        )

    assert (
        exception.value.args[0]
        == "The config attribute is not a subclass of GenericConnectorConfig"
    )


def test_connector_register_wrong_connector_type():
    class StubConnector:
        pass

    with pytest.raises(TypeError) as exception:
        romeways.connector_register(
            connector=StubConnector,
            config=GenericConnectorConfig(connector_name="test_connector_name"),
            spawn_process=False,
        )

    assert (
        exception.value.args[0]
        == "The connector attribute is not a subclass of AQueueConnector"
    )


def test_connector_register():
    guide_service_singleton_ref.clean_references()
    romeways.connector_register(
        connector=StubQueueConnector,
        config=GenericConnectorConfig(connector_name="test_connector_name"),
        spawn_process=False,
    )


@pytest.mark.asyncio
async def test_start():
    guide_service_singleton_ref.clean_references()
    with patch.object(GuideService, "start", return_value=None) as patch_start:
        with patch.object(GuideService, "end", return_value=None) as patch_end:
            with patch("asyncio.sleep", side_effect=BaseException):
                await romeways.start()
    patch_start.assert_called_once()
    patch_end.assert_called_once()
