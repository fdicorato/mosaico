"""
These tests require the connection to the server (localhost)
"""

import pytest
import logging as log
from mosaicolabs.comm import MosaicoClient
from mosaicolabs.enum import SequenceStatus


def test_invalid_host():
    with pytest.raises(
        ConnectionError,
        match="Connection to Flight server at invalid-address:0 failed on startup",
    ):
        MosaicoClient.connect(host="invalid-address", port=0, timeout=0)


def test_read_non_existing_sequence_topic(_client: MosaicoClient):
    log.info("Expected three (3) errors after this line...")
    assert _client.sequence_handler("non-existing-sequence") is None
    assert (
        _client.topic_handler(sequence_name="non-existing", topic_name="/topic") is None
    )

    # free resources
    _client.close()


def test_sequence_writer_not_in_context(_client: MosaicoClient):
    swriter = _client.sequence_create("new-sequence", metadata={})
    assert swriter.sequence_status() == SequenceStatus.Null
    with pytest.raises(
        RuntimeError, match="SequenceWriter must be used within a 'with' block."
    ):
        swriter._check_entered()

    # free resources
    _client.close()


def test_sequence_invalid_name(_client: MosaicoClient):
    sequence_name = "invalid/sequence/name"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the report condition
    with pytest.raises(ValueError, match="Invalid characters '/' in sequence name"):
        with _client.sequence_create(sequence_name, {}) as _:
            pass

    sequence_name = "/invalid/sequence/name"

    # It is necessary to make the exception propagate until the SequenceWriter.__exit__
    # which triggers the report condition
    with pytest.raises(ValueError, match="Invalid characters '/' in sequence name"):
        with _client.sequence_create(sequence_name, {}) as _:
            pass

    # free resources
    _client.close()
