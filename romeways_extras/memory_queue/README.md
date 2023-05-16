# Romeways memory queue

This is an extra package for romeways for more details access the [romeways](https://github.com/CenturyBoys/romeways) Github page.

## Configs

### Queue

The only difference between the `romeways.GenericQueueConfig` and `romeways_memory_queue.MemoryQueueConfig` are some default parameters.

```python
from dataclasses import dataclass, field

from romeways import GenericQueueConfig


@dataclass(slots=True, frozen=True)
class MemoryQueueConfig(GenericQueueConfig):
    connector_name: str
    frequency: float = field(default=0.25)
    max_chunk_size: int = field(default=10)
    sequential: bool = field(default=False)
```
### Connector

Obs: Because the package use the multiprocessing.Queue is allowed to register only one queue consumer.

```python
from dataclasses import dataclass
from multiprocessing import Queue

from romeways import GenericConnectorConfig

@dataclass(slots=True, frozen=True)
class MemoryConnectorConfig(GenericConnectorConfig):
    connector_name: str
    queue: Queue
```

## Use case

```python
from multiprocessing import Queue

import romeways

# Create a queue config
config_q = romeways.MemoryQueueConfig(
    connector_name="memory-dev1"
)

# Register a controller/consumer for the queue name
@romeways.queue_consumer(queue_name="fila.pagamentos.done", config=config_q)
async def controller(message: romeways.Message):
    print(message)

# Config the connector
queue = Queue()

config_p = romeways.MemoryConnectorConfig(connector_name="memory-dev1", queue=queue)

# Register a connector
romeways.connector_register(
    connector=romeways.MemoryQueueConnector, config=config_p, spawn_process=True
)

```
