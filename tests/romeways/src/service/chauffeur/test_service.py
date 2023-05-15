from datetime import datetime, timedelta
from unittest.mock import patch, call

import asyncio
import freezegun
import pytest

from romeways import GenericConnectorConfig, GenericQueueConfig
from romeways.src.domain.exceptions import ResendException
from romeways.src.domain.models.config.itinerary import Itinerary
from romeways.src.domain.models.config.map import RegionMap
from romeways.src.service import ChauffeurService

from tests.mocs.stubs.queue_connector import StubQueueConnector


def get_chauffeur_service(callback, sequential=False) -> ChauffeurService:
    queue_config = GenericQueueConfig(
        connector_name="test_connector_name",
        frequency=1,
        max_chunk_size=1,
        sequential=sequential,
    )

    chauffeur = ChauffeurService(
        queue_connector=StubQueueConnector(
            connector_config=GenericConnectorConfig(connector_name="test"),
            config=queue_config,
        ),
        itinerary=Itinerary(
            queue_name="test_queue_name", config=queue_config, callback=callback
        ),
    )
    return chauffeur


@pytest.mark.asyncio
async def test_clock_handler():
    async def callback(message):
        pass

    chauffeur_service = get_chauffeur_service(callback)

    with patch("asyncio.sleep", return_value=None) as patched_asyncio_sleep:
        with patch("logging.warning", return_value=None) as patched_logging_warning:
            with freezegun.freeze_time("2012-01-14T00:00:00.000"):
                await chauffeur_service._clock_handler()
            with freezegun.freeze_time("2012-01-14T00:00:00.500"):
                await chauffeur_service._clock_handler()
            with freezegun.freeze_time("2012-01-14T00:00:03.000"):
                await chauffeur_service._clock_handler()
    patched_asyncio_sleep.assert_has_calls([call(0), call(0.5), call(0)])
    patched_logging_warning.assert_called_with(
        "The queue handler for connector 'test_connector_name' and queue 'test_queue_name'"
        " are taken 2.5 seconds and your frequency is 1"
    )


@pytest.mark.asyncio
async def test_resolve_message_exception():
    async def callback(message):
        raise Exception("resend_error")

    chauffeur_service = get_chauffeur_service(callback)

    with patch("logging.error", return_value=None) as patched_logging_error:
        await chauffeur_service._resolve_message(message=b"10")

    patched_logging_error.assert_called_with(
        "A error occurs on handler callback for the connector 'test_connector_name'"
        "  and queue 'test_queue_name'. Error resend_error"
    )


@pytest.mark.asyncio
async def test_resolve_message_resend_exception():
    async def callback(message):
        raise ResendException("resend_error")

    chauffeur_service = get_chauffeur_service(callback)

    with patch("logging.error", return_value=None) as patched_logging_error:
        with patch.object(
            ChauffeurService, "_resend_message", return_value=None
        ) as patched_resend_message:
            await chauffeur_service._resolve_message(message=b"10")

    patched_logging_error.assert_called_with(
        "A error occurs on handler callback for the connector 'test_connector_name'"
        "  and queue 'test_queue_name'. The follow message will be resend Message(p"
        "ayload='10', rw_resend_times=0). Error resend_error"
    )
    patched_resend_message.assert_called_with(message=b"10")


@pytest.mark.asyncio
async def test_resend_message():
    async def callback(message):
        raise ResendException("resend_error")

    chauffeur_service = get_chauffeur_service(callback)
    with patch.object(
        StubQueueConnector, "send_messages", return_value=None
    ) as patched_send_messages:
        await chauffeur_service._resend_message(message=b"10")

    patched_send_messages.assert_called_with(b'{"payload": "10", "rw_resend_times": 1}')


@pytest.mark.asyncio
async def test_watch_sequential_false():
    message_call_sequence = []
    datetime_call_sequence = []

    async def callback(message):
        await asyncio.sleep(1)
        message_call_sequence.append(message)
        datetime_call_sequence.append(datetime.now())

    messages_bytes_a = b"10"
    messages_bytes_b = b"20"
    chauffeur_service = get_chauffeur_service(callback)
    with patch.object(
        StubQueueConnector,
        "get_messages",
        return_value=[messages_bytes_a, messages_bytes_b],
    ):
        task = asyncio.create_task(chauffeur_service._watch())
        await asyncio.sleep(1.1)
        task.cancel()

    message_a = message_call_sequence[0]
    message_b = message_call_sequence[1]
    assert message_a.payload == messages_bytes_a.decode()
    assert message_b.payload == messages_bytes_b.decode()

    datetime_a = datetime_call_sequence[0]
    datetime_a = datetime_a.replace(microsecond=0)
    datetime_b = datetime_call_sequence[1]
    datetime_b = datetime_b.replace(microsecond=0)

    assert datetime_b == datetime_a


@pytest.mark.asyncio
async def test_watch_sequential_true():
    message_call_sequence = []
    datetime_call_sequence = []

    async def callback(message):
        await asyncio.sleep(1)
        message_call_sequence.append(message)
        datetime_call_sequence.append(datetime.now())

    messages_bytes_a = b"10"
    messages_bytes_b = b"20"
    chauffeur_service = get_chauffeur_service(callback, True)
    with patch.object(
        StubQueueConnector,
        "get_messages",
        return_value=[messages_bytes_a, messages_bytes_b],
    ):
        task = asyncio.create_task(chauffeur_service._watch())
        await asyncio.sleep(2.1)
        task.cancel()

    message_a = message_call_sequence[0]
    message_b = message_call_sequence[1]
    assert message_a.payload == messages_bytes_a.decode()
    assert message_b.payload == messages_bytes_b.decode()

    datetime_a = datetime_call_sequence[0]
    datetime_a = datetime_a.replace(microsecond=0) + timedelta(seconds=1)
    datetime_b = datetime_call_sequence[1]
    datetime_b = datetime_b.replace(microsecond=0)

    assert datetime_b == datetime_a


@pytest.mark.asyncio
async def test_run():
    async def callback(message):
        pass

    itinerary = Itinerary(
        queue_name="test_queue_name",
        config=GenericQueueConfig(
            connector_name="test_connector_name",
            frequency=1,
            max_chunk_size=1,
            sequential=False,
        ),
        callback=callback,
    )

    region_map = RegionMap(
        spawn_process=False,
        config=GenericConnectorConfig(
            connector_name="test_connector_name",
        ),
        connector=StubQueueConnector,
    )

    with patch.object(ChauffeurService, "_watch", return_value=None) as path_watch:
        task = asyncio.create_task(
            ChauffeurService.run(
                region_map=region_map, itineraries=[itinerary, itinerary]
            )
        )
        await asyncio.sleep(1)
        task.cancel()

    assert path_watch.await_count == 2
