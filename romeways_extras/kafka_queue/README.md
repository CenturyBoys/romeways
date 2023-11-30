# Romeways kafka

This is an extra package for romeways for more details access the [romeways](https://github.com/CenturyBoys/romeways) Github page.

This package use [aiokafka](https://pypi.org/project/aiokafka/) to connect kafka server.

## Configs

### Queue


```python
from dataclasses import dataclass

from romeways import GenericQueueConfig


@dataclass(slots=True, frozen=True)
class KafkaQueueConfig(GenericQueueConfig):
    """
    topic: str See aiokafka.AIOKafkaConsumer doc
     -> https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaConsumer
    group_id: str See aiokafka.AIOKafkaConsumer doc.
     -> https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaConsumer
    """
    
    topic: str
    group_id: str
    connector_name: str
    frequency: float
    max_chunk_size: int
    sequential: bool
```
### Connector


```python
from dataclasses import dataclass

from romeways import GenericConnectorConfig


@dataclass(slots=True, frozen=True)
class KafkaConnectorConfig(GenericConnectorConfig):
    """
    bootstrap_server: str | list[str] See aiokafka.AIOKafkaConsumer doc
     -> https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaConsumer
    client_id: str See aiokafka.AIOKafkaConsumer doc.
     -> https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaConsumer
    """
        
    bootstrap_server: str
    client_id: str
```

## Use case

```python
import asyncio

import romeways

# Create a queue config
config_q = romeways.KafkaQueueConfig(
    topic="net_topic",
    group_id="my-project",
    connector_name="kafka-dev1",
    frequency=1,
    max_chunk_size=10,
    sequential=False,
)

# Register a controller/consumer for the queue name
@romeways.queue_consumer(queue_name="queue.payment.done", config=config_q)
async def controller(message: romeways.Message):
    print(message)

config_p = romeways.KafkaConnectorConfig(
    connector_name="kafka-dev1", 
    bootstrap_server="localhost:9094", 
    client_id="1"
)

# Register a connector
romeways.connector_register(
    connector=romeways.KafkaQueueConnector, config=config_p, spawn_process=True
)

asyncio.run(romeways.start())

```
