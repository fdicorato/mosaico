from enum import Enum


class SequenceStatus(Enum):
    """
    Represents the lifecycle state of a Sequence during the writing process.
    """

    Null = "null"  # Not yet initialized or registered on server.
    Pending = "pending"  # Registered on server; accepting data; not yet finalized.
    Finalized = "finalized"  # Successfully closed; data is immutable.
    Error = "error"  # Aborted or failed state.
