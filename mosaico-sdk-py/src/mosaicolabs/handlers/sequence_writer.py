"""
Sequence Writing Module.

This module acts as the central controller for writing a sequence of data.
It manages the lifecycle of the sequence on the server (Create -> Write -> Finalize)
and distributes client resources (Connections, Executors) to individual Topics.
"""

from typing import Any, Dict, Optional
import pyarrow.flight as fl

from .config import WriterConfig
from .helpers import _validate_sequence_name
from .internal.base_sequence_writer import _BaseSequenceWriter
from ..comm.do_action import _do_action, _DoActionResponseKey
from ..comm.connection import _ConnectionPool
from ..comm.executor_pool import _ExecutorPool
from ..enum import FlightAction, SequenceStatus
from ..logging import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


class SequenceWriter(_BaseSequenceWriter):
    """
    Specialized writer for the initial creation and population of a Mosaico Sequence.

    This class implements the lifecycle handshake required to register a brand-new
    sequence on the server. It leverages the 'Multi-Lane' architecture of the
    base class to provide high-performance, parallel data streaming across
    multiple topics.

    **Key Responsibilities:**
    1.  **Sequence Initialization**: Executes the `SEQUENCE_CREATE` action upon
        entering the execution context.
    2.  **Metadata Management**: Associates user-defined metadata with the new
        sequence during the creation phase.
    3.  **Resource Delegation**: Inherits the orchestration of network connections
        and thread executors to ensure isolated, non-blocking topic writes.
    """

    # -------------------- Class attributes --------------------
    _metadata: Dict[str, Any]

    # -------------------- Constructor --------------------
    def __init__(
        self,
        sequence_name: str,
        client: fl.FlightClient,
        connection_pool: Optional[_ConnectionPool],
        executor_pool: Optional[_ExecutorPool],
        metadata: dict[str, Any],
        config: WriterConfig,
    ):
        """
        Initializes a new SequenceWriter instance.

        .. note::
            This is an internal constructor. Application code should use
            `MosaicoClient.sequence_create()` to obtain a managed instance.

        Args:
            sequence_name (str): The semantic name for the new sequence.
            client (fl.FlightClient): The control-plane client for metadata actions.
            connection_pool (Optional[_ConnectionPool]): Shared pool for data streaming.
            executor_pool (Optional[_ExecutorPool]): Shared pool for async I/O.
            metadata (dict[str, Any]): Global metadata to attach to the sequence.
            config (WriterConfig): Writing policies (batching, error handling).
        """
        _validate_sequence_name(sequence_name)
        super().__init__(
            sequence_name=sequence_name,
            client=client,
            config=config,
            connection_pool=connection_pool,
            executor_pool=executor_pool,
            logger=logger,
        )
        self._metadata: dict[str, Any] = metadata

    # -------------------- Base class abstract method override --------------------
    def _on_context_enter(self):
        """
        Performs the server-side handshake to create the new sequence.

        Triggers the `SEQUENCE_CREATE` action, transmitting the sequence name
        and initial metadata. Upon success, it captures the unique authorization
        key required for subsequent topic creation.

        Raises:
            Exception: If the server rejects the creation or returns an empty response.
        """
        ACTION = FlightAction.SEQUENCE_CREATE

        act_resp = _do_action(
            client=self._control_client,
            action=ACTION,
            payload={
                "name": self._name,
                "user_metadata": self._metadata,
            },
            expected_type=_DoActionResponseKey,
        )

        if act_resp is None:
            raise Exception(f"Action '{ACTION.value}' returned no response.")

        self._key = act_resp.key
        self._entered = True
        self._sequence_status = SequenceStatus.Pending

    # NOTE: No need of overriding `_on_context_exit` as default behavior is ok.
