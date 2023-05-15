import asyncio
from multiprocessing import Process
from typing import List, Callable

from romeways.src.core.interfaces.infrastructure.spawner.interface import ISpawner
from romeways.src.domain.models.config.itinerary import Itinerary
from romeways.src.domain.models.config.map import RegionMap


class Spawner(ISpawner):
    def __init__(self, region_map: RegionMap, itineraries: List[Itinerary]):
        self._region_map = region_map
        self._itineraries = itineraries
        self._processes = []
        self._tasks = []

    async def start(self, callback_to_spawn: Callable):
        if self._region_map.spawn_process:
            await self._process_worker(
                region_map=self._region_map,
                itineraries=self._itineraries,
                callback_to_spawn=callback_to_spawn,
            )
        else:
            await self._async_worker(
                region_map=self._region_map,
                itineraries=self._itineraries,
                callback_to_spawn=callback_to_spawn,
            )

    def close(self):
        [process.kill() for process in self._processes]
        [task.cancel() for task in self._tasks]

    async def _async_worker(
        self,
        region_map: RegionMap,
        itineraries: List[Itinerary],
        callback_to_spawn: Callable,
    ):
        task = asyncio.create_task(
            callback_to_spawn(region_map=region_map, itineraries=itineraries)
        )
        self._tasks.append(task)

    async def _process_worker(
        self,
        region_map: RegionMap,
        itineraries: List[Itinerary],
        callback_to_spawn: Callable,
    ):
        def process_wrapper(_region_map, _itineraries):
            asyncio.run(
                callback_to_spawn(region_map=_region_map, itineraries=_itineraries)
            )

        process = Process(target=process_wrapper, args=(region_map, itineraries))
        process.start()
        self._processes.append(process)
