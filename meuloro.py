import asyncio
import json
import random
from multiprocessing import Queue

import romeways
from romeways import (
    MemoryConnectorConfig,
    MemoryQueueConnector,
    MemoryQueueConfig,
    ResendException,
    Message,
)


if __name__ == "__main__":

    # Create a queue config
    config_q = MemoryQueueConfig(
        connector_name="memory-dev1", max_chunk_size=2, frequency=2, sequential=False
    )

    # Regster a controller/resolver for the queue name
    @romeways.queue_consumer(queue_name="fila.pagamentos.done", config=config_q)
    async def controller(message: Message):
        time_to_await = random.uniform(0, 3.0)
        print("Eu sou a controller", message)
        await asyncio.sleep(time_to_await)

    # Config the connector
    queue = Queue()

    config_p = MemoryConnectorConfig(connector_name="memory-dev1", queue=queue)

    # Register a connector
    romeways.connector_register(
        connector=MemoryQueueConnector, config=config_p, spawn_process=True
    )

    async def xxx():
        task = asyncio.create_task(romeways.start())
        input_v = "1234567890"
        [queue.put(json.dumps({"number": int(char)}).encode()) for char in input_v]
        while True:
            await asyncio.sleep(5)
        task.cancel()

    asyncio.run(xxx())
