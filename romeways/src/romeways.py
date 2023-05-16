import asyncio
from typing import Callable, Type


from .core.abstract.infrastructure.queue_connector import AQueueConnector
from .domain.models.config.connector import GenericConnectorConfig
from .domain.models.config.queue import GenericQueueConfig
from .service.guide import GuideService


def queue_consumer(queue_name: str, config: GenericQueueConfig):

    if not isinstance(config, GenericQueueConfig):
        raise TypeError("The config attribute is not a subclass of GenericQueueConfig")

    def request_guide(func: Callable):
        if not asyncio.iscoroutinefunction(func):
            raise TypeError("The callback is not a coroutine function")
        GuideService().register_route(queue_name, config, func)
        return func

    return request_guide


def connector_register(
    connector: Type[AQueueConnector],
    config: GenericConnectorConfig,
    spawn_process: bool,
):
    if not isinstance(config, GenericConnectorConfig):
        raise TypeError(
            "The config attribute is not a subclass of GenericConnectorConfig"
        )

    if not issubclass(connector, AQueueConnector):
        raise TypeError("The connector attribute is not a subclass of AQueueConnector")

    GuideService().register_connector(
        connector=connector, config=config, spawn_process=spawn_process
    )


async def start():
    try:
        await GuideService().start()
        while True:
            await asyncio.sleep(10)
    except BaseException:  # pylint: disable=W0718
        GuideService().end()
