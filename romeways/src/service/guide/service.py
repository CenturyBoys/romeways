import asyncio
from typing import Callable, Type

import meeseeks

from romeways.src.core.abstract.infrastructure.queue_connector import AQueueConnector
from romeways.src.core.interfaces.service.guide import IGuide
from romeways.src.domain.models.config.connector.model import GenericConnectorConfig
from romeways.src.domain.models.config.itinerary import Itinerary
from romeways.src.domain.models.config.map import RegionMap
from romeways.src.domain.models.config.queue import GenericQueueConfig
from romeways.src.infrastructure.spawner import Spawner


@meeseeks.OnlyOne()
class GuideService(IGuide):
    def __init__(self):
        self.__itineraries = {}
        self.__region_maps = {}
        self.__spawners = []

    def register_route(
        self, queue_name: str, config: GenericQueueConfig, callback: Callable
    ):
        itinerary = Itinerary(
            queue_name=queue_name,
            config=config,
            callback=callback,
        )
        if config.connector_name not in self.__itineraries:
            self.__itineraries.update({config.connector_name: []})
        self.__itineraries[config.connector_name].append(itinerary)

    def register_connector(
        self,
        connector: Type[AQueueConnector],
        config: GenericConnectorConfig,
        spawn_process: bool,
    ):
        region_map = RegionMap(
            spawn_process=spawn_process, config=config, connector=connector
        )
        if config.connector_name not in self.__region_maps:
            self.__region_maps.update({config.connector_name: region_map})

    async def start(self):
        self.__spawners = [
            Spawner(
                region_map=self.__region_maps.get(connector_name),
                itineraries=self.__itineraries.get(connector_name, []),
            )
            for connector_name in self.__region_maps
        ]
        await asyncio.gather(*[spawner.start() for spawner in self.__spawners])

    def end(self):
        [spawner.close() for spawner in self.__spawners]
