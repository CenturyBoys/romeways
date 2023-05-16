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
