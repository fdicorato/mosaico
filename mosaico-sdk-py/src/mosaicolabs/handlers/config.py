"""
Configuration Module.

This module defines the configuration structures used to control the behavior
of the writing process, including error handling policies and batching limits.
"""

from dataclasses import dataclass
from ..enum import OnErrorPolicy


@dataclass
class WriterConfig:
    """
    Configuration settings for Sequence and Topic writers.

    Attributes:
        on_error (OnErrorPolicy): Determines action if a write fails (Report or Delete).
        max_batch_size_bytes (int): The threshold in bytes before a batch is flushed to the server.
        max_batch_size_records (int): The threshold in row count before a batch is flushed.
    """

    on_error: OnErrorPolicy
    max_batch_size_bytes: int
    max_batch_size_records: int
