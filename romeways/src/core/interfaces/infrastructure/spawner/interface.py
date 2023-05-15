from abc import ABC, abstractmethod
from typing import Callable


class ISpawner(ABC):
    @abstractmethod
    async def start(self, callback_to_spawn: Callable):
        pass

    @abstractmethod
    def close(self):
        pass
