# Romeways memory queue

This is an extra package for romeways for more details access the [romeways](https://github.com/CenturyBoys/romeways) Github page.

## Configs

### Queue

The only difference between the `romeways.GenericQueueConfig` and `romeways_memory_queue.MemoryQueueConfig` is the `multiprocessing.Queue` instance.

```python
from dataclasses import dataclass
from multiprocessing import Queue

from romeways import GenericQueueConfig


@dataclass(slots=True, frozen=True)
class MemoryQueueConfig(GenericQueueConfig):
    connector_name: str
    frequency: float
    max_chunk_size: int
    sequential: bool
    queue: Queue
```
### Connector


```python
from dataclasses import dataclass

from romeways import GenericConnectorConfig

@dataclass(slots=True, frozen=True)
class MemoryConnectorConfig(GenericConnectorConfig):
    connector_name: str
```

## Use case

```python
from multiprocessing import Queue
import asyncio

import romeways

# Config the connector
queue = Queue()

# Create a queue config
config_q = romeways.MemoryQueueConfig(
    connector_name="memory-dev1",
    frequency=1,
    max_chunk_size=10,
    sequential=False,
    queue=queue
)

# Register a controller/consumer for the queue name
@romeways.queue_consumer(queue_name="queue.payment.done", config=config_q)
async def controller(message: romeways.Message):
    print(message)



config_p = romeways.MemoryConnectorConfig(connector_name="memory-dev1")

# Register a connector
romeways.connector_register(
    connector=romeways.MemoryQueueConnector, config=config_p, spawn_process=True
)

asyncio.run(romeways.start())

```
