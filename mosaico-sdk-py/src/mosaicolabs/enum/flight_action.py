from enum import StrEnum


# --- Centralized Actions Enum ---
# This is now the single source of truth for all action names.
class FlightAction(StrEnum):
    # Sequences related
    SEQUENCE_CREATE = "sequence_create"
    SEQUENCE_FINALIZE = "sequence_finalize"
    SEQUENCE_NEW_VERSION = "sequence_new_version"
    SEQUENCE_NOTIFY_CREATE = "sequence_notify_create"
    SEQUENCE_SYSTEM_INFO = "sequence_system_info"
    SEQUENCE_ABORT = "sequence_abort"
    SEQUENCE_DELETE = "sequence_delete"
    # Topics related
    TOPIC_CREATE = "topic_create"
    TOPIC_NOTIFY_CREATE = "topic_notify_create"
    TOPIC_SYSTEM_INFO = "topic_system_info"
    TOPIC_DELETE = "topic_delete"
    # Layers related
    LAYER_LIST = "layer_list"
    LAYER_CREATE = "layer_create"
    LAYER_UPDATE = "layer_update"
    LAYER_DELETE = "layer_delete"
    # Queries related
    QUERY = "query"
