"""
Sequence Writing Module.

This module acts as the central controller for writing a sequence of data.
It manages the lifecycle of the sequence on the server (Create -> Write -> Finalize)
and distributes client resources (Connections, Executors) to individual Topics.
"""

from abc import ABC, abstractmethod
from logging import Logger
from typing import Any, Dict, Type, Optional
import pyarrow.flight as fl

from ..config import WriterConfig
from ..helpers import _make_exception
from ..topic_writer import TopicWriter
from mosaicolabs.comm.do_action import _do_action, _DoActionResponseKey
from mosaicolabs.comm.connection import _ConnectionPool
from mosaicolabs.comm.executor_pool import _ExecutorPool
from mosaicolabs.enum import FlightAction, OnErrorPolicy, SequenceStatus
from mosaicolabs.helpers import pack_topic_resource_name
from mosaicolabs.models import Serializable


class _BaseSequenceWriter(ABC):
    """
    Abstract base class orchestrating high-performance data writing to Mosaico.

    This class implements the 'Multi-Lane' architecture, acting as a central
    controller that manages the distribution of shared client resources
    (Connection and Executor pools) across multiple isolated `TopicWriter` instances.

    **Key Responsibilities:**
    1.  **Resource Orchestration**: Assigns dedicated network connections and
        threading executors to individual topics, ensuring that high-bandwidth
        streams (e.g., video) do not block low-latency telemetry.
    2.  **Lifecycle Template**: Implements the logic for the context manager
        (`__enter__` / `__exit__`), providing hooks for subclasses to trigger
        specific server-side actions (e.g., creating a new sequence vs.
        creating a new version).
    3.  **Topic Management**: Tracks active `TopicWriter` instances and ensures
        synchronized finalization or rollback across all streams.

    **Implementation Note:**
    This class follows the Template Method pattern. Subclasses must implement
    `_on_context_enter` to define the initial server handshake.
    Default closing operations (called on __exit__ execution) are performed by this class.
    To customize such operations, user must override `_on_context_exit` method
    """

    # -------------------- Class attributes --------------------
    _name: str
    _topic_writers: Dict[str, TopicWriter]
    _control_client: fl.FlightClient
    """The FlightClient used for metadata operations (creating topics, finalizing sequence)."""

    _connection_pool: Optional[_ConnectionPool]
    """The pool of FlightClients available for data streaming."""

    _executor_pool: Optional[_ExecutorPool]
    """The pool of ThreadPoolExecutors available for asynch I/O."""

    _config: WriterConfig
    """Configuration object containing error policies and batch size limits."""

    _sequence_status: SequenceStatus = SequenceStatus.Null
    _key: Optional[str] = None
    _entered: bool = False

    # -------------------- Constructor --------------------
    def __init__(
        self,
        sequence_name: str,
        client: fl.FlightClient,
        connection_pool: Optional[_ConnectionPool],
        executor_pool: Optional[_ExecutorPool],
        config: WriterConfig,
        logger: Logger,
    ):
        """
        Internal constructor. Use `MosaicoClient.sequence_create()` instead.
        """
        self._name: str = sequence_name
        self._config = config
        self._topic_writers: Dict[str, TopicWriter] = {}
        self._control_client = client
        self._connection_pool = connection_pool
        self._executor_pool = executor_pool
        self._logger = logger

    @abstractmethod
    def _on_context_enter(self):
        pass

    def _on_context_exit(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """
        Executes the default finalization and cleanup logic for the sequence.

        **Summary of Operations:**
        1.  **Detection**: Determines if the context was exited due to an error
            or a successful completion.
        2.  **Topic Orchestration**:
            - **On Success**: Triggers a normal `finalize()` on all active `TopicWriter`
              instances to flush remaining buffers.
            - **On Failure**: Triggers an error-mode `finalize()` on all topics to
              ensure immediate resource release without data integrity guarantees.
        3.  **Server Lifecycle Handshake**:
            - **Success Path**: Calls `self.close()` to send a finalization signal
              (default: `SEQUENCE_FINALIZE`) to the server.
            - **Error Path**: Evaluates the `WriterConfig.on_error` policy to either
              `_abort()` (delete the sequence) or `_error_report()` to the server.
        4.  **Status Integrity**: Updates the internal `_sequence_status` to `Error`
            if any part of the process fails.

        **Notes:**
        - Override this method if your specific sequence implementation requires
        custom teardown logic that differs from the standard Mosaico flow.
        Examples include:
            - Releasing additional local resources (file handles, hardware locks).
            - Implementing a different "commit" logic for the sequence.
            - Sending specialized notification signals to the server upon exit.

        Args:
            exc_type: The type of the exception raised in the with-block, if any.
            exc_val: The instance of the exception raised in the with-block, if any.
            exc_tb: The traceback of the exception raised in the with-block, if any.
        """
        error_in_block = exc_type is not None
        out_exc = exc_val

        if not error_in_block:
            try:
                # Normal Exit: Finalize everything
                self._close_topics(with_error=False)
                self.close()

            except Exception as e:
                # An exception occurred during cleanup or finalization
                self._logger.error(
                    f"Exception during __exit__ for sequence '{self._name}': '{e}'"
                )
                # notify error and go on
                out_exc = e
                error_in_block = True

        if error_in_block:  # either in with block or after close operations
            # Exception occurred: Clean up and handle policy
            self._logger.error(
                f"Exception in SequenceWriter '{self._name}' block. Inner err: '{out_exc}'"
            )
            try:
                self._close_topics(with_error=True)
            except Exception as e:
                self._logger.error(
                    f"Exception during __exit__ with error in block (finalizing topics) for sequence '{self._name}': '{e}'"
                )
                out_exc = e

            # Apply the sequence-level error policy
            if self._config.on_error == OnErrorPolicy.Delete:
                self._abort()
            else:
                self._error_report(str(out_exc))

            # Last thing to do: DO NOT SET BEFORE!
            self._sequence_status = SequenceStatus.Error

            if exc_type is None and out_exc is not None:
                raise out_exc  # Re-raise the cleanup error if it's the only one

    # --- Context Manager ---
    def __enter__(self) -> "_BaseSequenceWriter":
        """
        Activates the sequence writer and initializes server-side resources.

        This method executes the specific lifecycle hook defined by the subclass
        (via `_on_sequence_open`) to prepare the server for data intake. Once
        initialized, the writer enters a 'Pending' state, enabling the creation
        of topics and parallel data streaming.

        Returns:
            _BaseSequenceWriter: The initialized and ready-to-write instance.
        """
        self._on_context_enter()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """
        Finalizes the sequence.

        - If successful: Finalizes all topics and the sequence itself.
        - If error: Finalizes topics in error mode and either Aborts (Delete)
          or Reports the error based on `WriterConfig.on_error`.
        """
        return self._on_context_exit(
            exc_type=exc_type,
            exc_val=exc_val,
            exc_tb=exc_tb,
        )

    def __del__(self):
        """Destructor check to warn if the writer was left pending."""
        name = getattr(self, "_name", "__not_initialized__")
        status = getattr(self, "_sequence_status", SequenceStatus.Null)

        if status == SequenceStatus.Pending:
            self._logger.warning(
                f"SequenceWriter '{name}' destroyed without calling close(). "
                "Resources may not have been released properly."
            )

    def _check_entered(self):
        """Ensures methods are only called inside a `with` block."""
        if not self._entered:
            raise RuntimeError("SequenceWriter must be used within a 'with' block.")

    # --- Public API ---
    def topic_create(
        self,
        topic_name: str,
        metadata: dict[str, Any],
        ontology_type: Type[Serializable],
    ) -> Optional[TopicWriter]:
        """
        Creates a new topic within the sequence.

        This method assigns a dedicated connection and executor from the pool
        (if available) to the new topic, enabling parallel writing.

        Args:
            topic_name (str): The name of the new topic.
            metadata (dict[str, Any]): Topic-specific metadata.
            ontology_type (Type[Serializable]): The data model class.

        Returns:
            TopicWriter: A writer instance configured for this topic.
            None: If any error occurs


        """
        ACTION = FlightAction.TOPIC_CREATE
        self._check_entered()

        if topic_name in self._topic_writers:
            self._logger.error(f"Topic '{topic_name}' already exists in this sequence.")
            return None

        self._logger.debug(
            f"Requesting new topic '{topic_name}' for sequence '{self._name}'"
        )

        try:
            # Register topic on server
            act_resp = _do_action(
                client=self._control_client,
                action=ACTION,
                payload={
                    "sequence_key": self._key,
                    "name": pack_topic_resource_name(self._name, topic_name),
                    "serialization_format": ontology_type.__serialization_format__.value,
                    "ontology_tag": ontology_type.__ontology_tag__,
                    "user_metadata": metadata,
                },
                expected_type=_DoActionResponseKey,
            )
        except Exception as e:
            self._logger.error(
                str(
                    _make_exception(
                        f"Failed to execute '{ACTION.value}' action for sequence '{self._name}', topic '{topic_name}'.",
                        e,
                    )
                )
            )
            return None

        if act_resp is None:
            self._logger.error(f"Action '{ACTION.value}' returned no response.")
            return None

        # --- Resource Assignment Strategy ---
        if self._connection_pool:
            # Round-Robin assignment from the pool (Async mode)
            data_client = self._connection_pool.get_next()
        else:
            # Reuse control client (Sync mode)
            data_client = self._control_client

        # Assign executor if pool is available
        executor = self._executor_pool.get_next() if self._executor_pool else None

        try:
            writer = TopicWriter.create(
                sequence_name=self._name,
                topic_name=topic_name,
                topic_key=act_resp.key,
                client=data_client,
                executor=executor,
                metadata=metadata,
                ontology_type=ontology_type,
                config=self._config,
            )
            self._topic_writers[topic_name] = writer

        except Exception as e:
            self._logger.error(
                str(
                    _make_exception(
                        f"Failed to initialize 'TopicWriter' for sequence '{self._name}', topic '{topic_name}'. Topic will be deleted from db.",
                        e,
                    )
                )
            )
            try:
                _do_action(
                    client=self._control_client,
                    action=FlightAction.TOPIC_DELETE,
                    payload={"name": pack_topic_resource_name(self._name, topic_name)},
                    expected_type=None,
                )
            except Exception:
                self._logger.error(
                    str(
                        _make_exception(
                            f"Failed to send TOPIC_DELETE do_action for sequence '{self._name}', topic '{topic_name}'.",
                            e,
                        )
                    )
                )
            return None

        return writer

    def sequence_status(self) -> SequenceStatus:
        """Returns the current status of the sequence."""
        return self._sequence_status

    def close(self):
        """
        Explicitly finalizes the sequence.

        Sends `SEQUENCE_FINALIZE` to the server, marking data as immutable.
        """
        self._check_entered()
        if self._sequence_status == SequenceStatus.Pending:
            try:
                _do_action(
                    client=self._control_client,
                    action=FlightAction.SEQUENCE_FINALIZE,
                    payload={
                        "name": self._name,
                        "key": self._key,
                    },
                    expected_type=None,
                )
                self._sequence_status = SequenceStatus.Finalized
                self._logger.info(f"Sequence '{self._name}' finalized successfully.")
                return
            except Exception as e:
                # _do_action raised: re-raise
                self._sequence_status = SequenceStatus.Error  # Sets status to Error
                raise _make_exception(
                    f"Error sending 'finalize' action for sequence '{self._name}'. Server state may be inconsistent.",
                    e,
                )

    def _error_report(self, err: str):
        """Internal: Sends error report to server."""
        if self._sequence_status == SequenceStatus.Pending:
            try:
                _do_action(
                    client=self._control_client,
                    action=FlightAction.SEQUENCE_NOTIFY_CREATE,
                    payload={
                        "name": self._name,
                        "notify_type": "error",
                        "msg": str(err),
                    },
                    expected_type=None,
                )
                self._logger.info(f"Sequence '{self._name}' reported error.")
            except Exception as e:
                raise _make_exception(
                    f"Error sending 'sequence_report_error' for '{self._name}'.", e
                )

    def _abort(self):
        """Internal: Sends Abort command (Delete policy)."""
        if self._sequence_status != SequenceStatus.Finalized:
            try:
                _do_action(
                    client=self._control_client,
                    action=FlightAction.SEQUENCE_ABORT,
                    payload={
                        "name": self._name,
                        "key": self._key,
                    },
                    expected_type=None,
                )
                self._logger.info(f"Sequence '{self._name}' aborted successfully.")
                self._sequence_status = SequenceStatus.Error
            except Exception as e:
                raise _make_exception(
                    f"Error sending 'abort' for sequence '{self._name}'.", e
                )

    def topic_exists(self, topic_name: str) -> bool:
        """Checks if a local TopicWriter exists for the name."""
        return topic_name in self._topic_writers

    def list_topics(self) -> list[str]:
        """Returns list of active topic names."""
        return [k for k in self._topic_writers.keys()]

    def get_topic(self, topic_name: str) -> Optional[TopicWriter]:
        """Retrieves a TopicWriter instance, if it exists."""
        return self._topic_writers.get(topic_name)

    def _close_topics(self, with_error: bool) -> None:
        """
        Iterates over all TopicWriters and finalizes them.
        """
        self._logger.info(
            f"Freeing TopicWriters {'WITH ERROR' if with_error else ''} for sequence '{self._name}'."
        )
        errors = []
        for topic_name, twriter in self._topic_writers.items():
            try:
                twriter.finalize(with_error=with_error)
            except Exception as e:
                self._logger.error(f"Failed to finalize topic '{topic_name}': '{e}'")
                errors.append(e)

        # Delete all TopicWriter instances, nothing can be done from here on
        self._topic_writers = {}

        if errors:
            first_error = errors[0]
            raise _make_exception(
                f"Errors occurred closing topics: {len(errors)} topic(s) failed to finalize.",
                first_error,
            )
