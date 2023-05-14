from abc import ABC, abstractmethod


class ISpawner(ABC):
    @abstractmethod
    async def start(self):
        pass

    @abstractmethod
    def close(self):
        pass
