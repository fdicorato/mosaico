from enum import Enum


class OnErrorPolicy(Enum):
    """
    Defines the behavior when an exception occurs during a sequence write.
    """

    Report = "report"  # Notify the server of the error but keep partial data.
    Delete = "delete"  # Abort the sequence and instruct server to discard all data.
