"""
Configuration Module.

This module defines the configuration structures used to control the behavior
of the writing process, including error handling policies and batching limits.
"""

from typing import Optional
from mosaicolabs.comm.connection import (
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_BATCH_SIZE_RECORDS,
)
from ..enum import OnErrorPolicy


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

    def __init__(
        self,
        on_error: Optional[OnErrorPolicy] = None,
        max_batch_size_bytes: Optional[int] = None,
        max_batch_size_records: Optional[int] = None,
    ) -> None:
        self.on_error = on_error or OnErrorPolicy.Delete
        self.max_batch_size_bytes = max_batch_size_bytes or DEFAULT_MAX_BATCH_BYTES
        self.max_batch_size_records = (
            max_batch_size_records or DEFAULT_MAX_BATCH_SIZE_RECORDS
        )
