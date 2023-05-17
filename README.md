# Romeways

<img src="./docs/images/banner.png" width="200"/>
by: CenturyBoys

This project has as goal help developers to not reimplemented default queue consumer behaviour.

# Basics

Romeways works with two basic concepts queue handler and queue connector. The queue connector is a queue consumer and can be spawned in a separate process or in async worker. The queue handler is the callback function that will be called for each retrieved message.

Here you can see all implemented consumer:

| Queue Type            | Install using extra | description                                    |
|-----------------------|---------------------|------------------------------------------------|
| multiprocessing.Queue | memory              | [here](romeways_extras/memory_queue/README.md) |

How to install extra packages?

```shell
poetry add romeways -E memory
OR
pip install 'romeways[memory]'
```

# Configuration

## Queue connector config

The queue connector config is all configurations that you need to be able to retrieve messages from the queue.

Bellow are the `romeways.GenericConnectorConfig` implementation. This class can be inheritance to allow extra configurations.

#### Params:

- `connector_name: str` For what connector this queue must be delivered

```python
from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class GenericConnectorConfig:
    """
    connector_name: str Connector name
    """
    connector_name: str

```

## Queue handler config

When you register a queue consumer you are setting configs and a callback handler for each message that this queue receives.

Bellow are the `romeways.GenericQueueConfig` implementation. This class can be inheritance to allow extra configurations.

#### Params:

- `connector_name: str` For what connector this queue must be delivered
- `frequency: float` Time in seconds for retrieve messages from queue
- `max_chunk_size: int` Max quantity for messages that one retrieve will get
- `sequential: bool` If the handler call must be sequential or in asyncio.gather

```python
from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class GenericQueueConfig:
    """
    connector_name: str For what connector this queue must be delivered
    frequency: float Time in seconds for retrieve messages from queue
    max_chunk_size: int Max quantity for messages that one retrieve will get
    sequential: bool If the handler call must be sequential or in asyncio.gather
    """
    connector_name: str
    frequency: float
    max_chunk_size: int
    sequential: bool

```

## Resend on error

Romeways allow you to resend the message to the queue if something in your handler do not perform correctly. For that your code need tho raise the `romeways.ResendException` exception, the message will be resent to the same queue and the `romeways.Message.rw_resend_times` parameter will be raized


## Spawn a process

Romeways can run each connector in a separate process or in async workers for that use the parameter `spawn_process` to configure that.

# Example

For this example we are using the extra package `memory`

```python
from multiprocessing import Queue

import romeways

# Config the connector
queue = Queue()

# Create a queue config
config_q = romeways.MemoryQueueConfig(
    connector_name="memory-dev1", 
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

```