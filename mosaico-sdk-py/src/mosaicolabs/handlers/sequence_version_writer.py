"""
Sequence Version Writing Module.

This module acts as the central controller for writing a new version of the sequence of data.
It manages the lifecycle of the sequence on the server (Create -> Write -> Finalize)
and distributes client resources (Connections, Executors) to individual Topics.
"""

from copy import deepcopy
from typing import Optional
import pyarrow.flight as fl

from .config import WriterConfig
from .helpers import _make_exception
from ..helpers.helpers import pack_topic_resource_name
from .internal.base_sequence_writer import _BaseSequenceWriter
from ..comm.do_action import _do_action, _DoActionResponseKey
from ..comm.connection import _ConnectionPool
from ..comm.executor_pool import _ExecutorPool
from ..enum import FlightAction, SequenceStatus
from ..logging import get_logger
from ..models.platform.sequence import Sequence

# Set the hierarchical logger
logger = get_logger(__name__)


class SequenceVersionWriter(_BaseSequenceWriter):
    """
    Specialized writer for creating and managing a new version of an existing Sequence.

    This class implements the lifecycle handshake required to initiate a versioned
    update (or "iteration") on the server. It allows for data curation by removing
    outdated topics and supports the addition of new data.

    **Key Responsibilities:**
    1.  **Versioning Handshake**: Executes the `SEQUENCE_NEW_VERSION` action upon
        entering the context, optionally associating the new version with a tag.
    2.  **Curation Interface**: Provides methods to selectively remove existing
        topics from the new version's scope.
    3.  **Parallel Data Intake**: Orchestrates shared network and threading resources
        to allow high-throughput writing to new or existing topics within the version.
    """

    # -------------------- Class attributes --------------------
    _sequence_name: str
    _tag_name: Optional[str]
    _available_topics: list[str]

    # -------------------- Constructor --------------------
    def __init__(
        self,
        sequence_model: Sequence,
        tag_name: Optional[str],
        client: fl.FlightClient,
        connection_pool: Optional[_ConnectionPool],
        executor_pool: Optional[_ExecutorPool],
        config: WriterConfig,
    ):
        """
        Initializes a new SequenceVersionWriter instance.

        .. note::
            This is an internal constructor. Application code should use
            `SequenceHandler.new_version()` to obtain a managed instance for
            versioning an existing sequence.

        Args:
            sequence_model (Sequence): The model representing the existing sequence.
            tag_name (Optional[str]): An optional semantic label (e.g., 'v2.0')
                for the new version.
            client (fl.FlightClient): Control-plane client for metadata operations.
            connection_pool (Optional[_ConnectionPool]): Shared pool for data streaming.
            executor_pool (Optional[_ExecutorPool]): Shared pool for async I/O.
            config (WriterConfig): Policies for batching and error handling.
        """
        # Init parent class
        super().__init__(
            sequence_name=sequence_model.name,
            client=client,
            config=config,
            connection_pool=connection_pool,
            executor_pool=executor_pool,
            logger=logger,
        )
        # Init local class members
        self._tag_name: Optional[str] = tag_name
        self._sequence_name = sequence_model.name
        # NOTE: use deepcopy, since this list may undergo changes in this class
        self._available_topics = deepcopy(sequence_model.topics)

    # -------------------- Base class abstract method override --------------------
    def _on_context_enter(self):
        """
        Performs the server-side handshake to initiate a new sequence version.

        Triggers the `SEQUENCE_NEW_VERSION` action, transmitting the sequence
        name and the optional version tag. Upon
        success, it captures the unique authorization key required to modify the
        sequence or write new topic data.

        Raises:
            Exception: If the server rejects the version creation or returns
                an empty response.
        """
        ACTION = FlightAction.SEQUENCE_NEW_VERSION

        act_payload = {
            "name": self._name,
        }
        if self._tag_name is not None:
            act_payload.update({"tag_name": self._tag_name})

        act_resp = _do_action(
            client=self._control_client,
            action=ACTION,
            payload=act_payload,
            expected_type=_DoActionResponseKey,
        )

        if act_resp is None:
            raise Exception(f"Action '{ACTION.value}' returned no response.")

        self._key = act_resp.key
        self._entered = True
        self._sequence_status = SequenceStatus.Pending

    # NOTE: No need of overriding `_on_context_exit` as default behavior is ok.

    def topic_delete(self, topic_name: str):
        """
        Requests the removal of a topic from the current sequence version.

        This operation instructs the server to exclude the specified topic from
        the version context. Deleted topics are
        immediately removed from the local `available_topics` list to maintain
        client-side consistency.

        Args:
            topic_name (str): The name of the topic to remove.

        Raises:
            ValueError: If the topic does not exist within the parent sequence.
        """
        if topic_name not in self._available_topics:
            raise ValueError(
                f"Topic {topic_name} does not belong to the sequence {self._sequence_name}."
            )
        try:
            _do_action(
                client=self._control_client,
                action=FlightAction.TOPIC_DELETE,
                payload={"name": pack_topic_resource_name(self._name, topic_name)},
                expected_type=None,
            )
            # Coherency: Remove this topic from the list of available ones
            self._available_topics.remove(topic_name)
        except Exception as e:
            self._logger.error(
                str(
                    _make_exception(
                        f"Failed to send TOPIC_DELETE do_action for sequence '{self._name}', topic '{topic_name}'.",
                        e,
                    )
                )
            )

    @property
    def available_topics(self) -> list[str]:
        """
        The list of topics currently available in this version.

        This list reflects the state of the version, excluding any topics
        removed via `topic_delete` but including existing topics inherited
        from the previous version.
        """
        return self._available_topics
