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


class ChauffeurService(IChauffeur):
    @classmethod
    async def run(cls, region_map: RegionMap, itineraries: List[Itinerary]):
        queue_connector_class: Type[AQueueConnector] = region_map.connector
        chauffeurs = []
        for itinerary in itineraries:
            queue_connector = queue_connector_class(
                connector_config=region_map.config,
                queue_name=itinerary.queue_name,
                config=itinerary.config,
            )
            await queue_connector.on_start()
            chauffeur = cls(queue_connector=queue_connector, itinerary=itinerary)
            chauffeurs.append(chauffeur.watch())
        await asyncio.gather(*chauffeurs)

    def __init__(self, queue_connector: AQueueConnector, itinerary: Itinerary):
        self._queue_connector = queue_connector
        self._itinerary = itinerary
        self._x = None

    async def _clock_handler(self):
        queue_config: GenericQueueConfig = self._itinerary.config
        await_time = queue_config.frequency
        if self._x is None:
            await_time = 0
        else:
            time_now = time()
            delta = time_now - self._x
            await_time = await_time - delta
            if await_time < 0:
                queue_name: str = self._itinerary.queue_name
                logging.warning(
                    f"The queue handler for connector '{queue_config.connector_name}' and queue '{queue_name}'"
                    f" are taken {float(delta)} seconds and your frequency is {queue_config.frequency}"
                )
        await asyncio.sleep(await_time)
        self._x = time()

    async def _resolve_message(self, message: bytes):
        callback = self._itinerary.callback
        message_obj = Message.from_message(message=message)
        try:
            await callback(message_obj)
        except ResendException as error:
            queue_config: GenericQueueConfig = self._itinerary.config
            logging.error(
                f"A error occurs on handler {callback} for the connector '{queue_config.connector_name}' "
                f" and queue '{self._itinerary.queue_name}'. The follow message will be resend {message_obj}."
                f" Error {error}"
            )
            await self._resend_message(message=message)
        except BaseException as error:
            queue_config: GenericQueueConfig = self._itinerary.config
            logging.error(
                f"A error occurs on handler {callback} for the connector '{queue_config.connector_name}' "
                f" and queue '{self._itinerary.queue_name}'. Error {error}"
            )

    async def _resend_message(self, message: bytes):
        message_obj = Message.from_message(message=message)
        message_obj.rw_resend_times += 1
        await self._queue_connector.send_messages(message_obj.toJSON())

    async def watch(self):
        while True:
            await self._clock_handler()
            queue_config: GenericQueueConfig = self._itinerary.config
            messages: List[bytes] = await self._queue_connector.get_messages(
                max_chunk_size=queue_config.max_chunk_size
            )
            if queue_config.sequential:
                [await self._resolve_message(message) for message in messages]
            else:
                await asyncio.gather(
                    *[self._resolve_message(message) for message in messages]
                )
