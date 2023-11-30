import asyncio
import logging
from time import time
from typing import List, Type

from romeways.src.core.abstract.infrastructure.queue_connector import AQueueConnector
from romeways.src.core.interfaces.service.chauffeur import IChauffeur
from romeways.src.domain.exceptions import ResendException
from romeways.src.domain.models.config.itinerary import Itinerary
from romeways.src.domain.models.config.map import RegionMap
from romeways.src.domain.models.config.queue import GenericQueueConfig
from romeways.src.domain.models.message import Message


class Empty:
    pass


class ChauffeurService(IChauffeur):
    @classmethod
    async def run(cls, region_map: RegionMap, itineraries: List[Itinerary]):
        chauffeurs = []
        for itinerary in itineraries:
            queue_connector = region_map.connector(
                connector_config=region_map.config,
                config=itinerary.config,
            )
            await queue_connector.on_start()
            chauffeur = cls(queue_connector=queue_connector, itinerary=itinerary)
            chauffeurs.append(chauffeur._watch())
        await asyncio.gather(*chauffeurs)

    def __init__(self, queue_connector: AQueueConnector, itinerary: Itinerary):
        self._queue_connector = queue_connector
        self._itinerary = itinerary
        self._last_execution_time = None

    async def _clock_handler(self):
        queue_config: GenericQueueConfig = self._itinerary.config
        await_time = queue_config.frequency
        if self._last_execution_time:
            delta = time() - self._last_execution_time
            await_time = await_time - delta
            if await_time < 0.0:
                queue_name: str = self._itinerary.queue_name
                logging.warning(
                    "The queue handler for connector %s and queue %s "
                    "are taken %s seconds and your frequency is %s",
                    queue_config.connector_name,
                    queue_name,
                    float(delta),
                    queue_config.frequency,
                )
                await_time = 0.0
        else:
            await_time = 0.0
        await asyncio.sleep(await_time)
        self._last_execution_time = time()

    async def _resolve_message(self, message: bytes):
        message_obj = Message.from_message(message=message)
        try:
            await self._itinerary.callback(message_obj)
        except ResendException as exception:
            queue_config: GenericQueueConfig = self._itinerary.config
            logging.error(
                "A error occurs on handler %s for the connector %s and queue %s."
                " The follow message will be resend %s. Error %s",
                self._itinerary.callback.__name__,
                queue_config.connector_name,
                self._itinerary.queue_name,
                message_obj,
                exception,
            )
            await self._resend_message(message=message)
        except BaseException as exception:  # pylint: disable=W0718
            queue_config: GenericQueueConfig = self._itinerary.config
            logging.error(
                "A error occurs on handler %s for the connector %s and queue %s. Error %s",
                self._itinerary.callback.__name__,
                queue_config.connector_name,
                self._itinerary.queue_name,
                exception,
            )

    async def _resend_message(self, message: bytes):
        message_obj = Message.from_message(message=message)
        message_obj.rw_resend_times += 1
        await self._queue_connector.send_messages(message_obj.to_json())

    async def _watch(self):
        while True:
            await self._clock_handler()
            messages: List[bytes] = await self._queue_connector.get_messages(
                max_chunk_size=self._itinerary.config.max_chunk_size
            )
            if self._itinerary.config.sequential is False:
                await asyncio.gather(
                    *[self._resolve_message(message) for message in messages]
                )
            else:
                for message in messages:
                    await self._resolve_message(message)
