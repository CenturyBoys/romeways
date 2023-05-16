from abc import abstractmethod, ABC
from typing import List

from romeways.src.core.abstract.infrastructure.queue_connector import AQueueConnector
from romeways.src.domain.models.config.itinerary import Itinerary
from romeways.src.domain.models.config.map import RegionMap


class IChauffeur(ABC):
    queue_connector: AQueueConnector
    itinerary: Itinerary

    @classmethod
    @abstractmethod
    async def run(cls, region_map: RegionMap, itineraries: List[Itinerary]):
        pass
