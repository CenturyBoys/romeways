from abc import abstractmethod, ABC
from typing import List

from romeways.src.domain.models.config.itinerary import Itinerary
from romeways.src.domain.models.config.map import RegionMap


class IChauffeur(ABC):
    @classmethod
    @abstractmethod
    async def run(cls, region_map: RegionMap, itineraries: List[Itinerary]):
        pass
