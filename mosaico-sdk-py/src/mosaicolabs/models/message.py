"""
Message Envelope Module.

This module defines the `Message` class, which acts as the transport envelope
for all ontology data. It wraps the specific ontology payload (`data`) with
middleware-level metadata (like recording timestamp_ns).

"""

# --- Python Standard Library Imports ---
from typing import Any, Dict, Optional, Type, TypeVar
from pydantic import PrivateAttr
import pyarrow as pa

from ..helpers.helpers import encode_to_dict
from .header import Header
from .serializable import Serializable, _SENSOR_REGISTRY
from .internal.helpers import _fix_empty_dicts
from .base_model import BaseModel


def _make_schema(*args: pa.StructType) -> pa.Schema:
    """Helper to merge multiple PyArrow structs into a single Schema."""
    return pa.schema([field for struct in args for field in struct])


TSensor = TypeVar("TSensor", bound="Serializable")


class Message(BaseModel):
    """
    The universal container for data transmission.

    Attributes:
        timestamp_ns (int): Middleware processing timestamp in nanoseconds (different from sensor acquisition time).
        message_header (Optional[Header]): Middleware-level header.
        data (Serializable): The polymorphic payload (e.g., an IMU object).
    """

    # Define the Message schema (Envelope fields only)
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "timestamp_ns",
                pa.int64(),
                nullable=False,
                metadata={
                    "description": "Middleware processing timestamp in nanoseconds (e.g., recording time)."
                },
            ),
            pa.field(
                "message_header",
                Header.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "Optional middleware header."},
            ),
        ]
    )

    # Pydantic definitions
    timestamp_ns: int
    data: Serializable
    message_header: Optional[Header] = None

    # Internal cache for efficient field separation during encoding
    _self_model_keys: set[str] = PrivateAttr(default_factory=set)
    _data_model_keys: set[str] = PrivateAttr(default_factory=set)

    def model_post_init(self, context: Any) -> None:
        """
        Validates that the 'data' payload does not have fields that collide
        with the Message envelope fields (e.g., 'timestamp_ns').
        """
        super().model_post_init(context)
        self._self_model_keys = {
            field for field in self.__class__.model_fields if field != "data"
        }
        self._data_model_keys = {field for field in self.data.__class__.model_fields}

        colliding_fields = self._self_model_keys & self._data_model_keys
        if colliding_fields:
            raise ValueError(
                f"Fields name collision detected between class '{type(self.data).__name__}' "
                f"and Message envelope. Colliding fields: {colliding_fields}."
            )

    def ontology_type(self) -> Type[Serializable]:
        """Retrieves the Python class type of the ontology object stored in the data field."""
        return self.data.__class_type__

    def ontology_tag(self) -> str:
        """Returns the unique ontology tag name associated with the object in the data field."""
        return getattr(
            self.data, "__ontology_tag__"
        )  # avoid the IDE complaining (__ontology_tag__ defined as Optional but surely not None at this point)

    def encode(self) -> Dict[str, Any]:
        """
        Flattens the object into a dictionary suitable for PyArrow serialization.

        Merges the Message fields ('timestamp_ns') and the Data fields
        into a single flat dictionary.
        """
        # Encode envelope fields
        columns_dict = {
            field: encode_to_dict(getattr(self, field))
            for field in self._self_model_keys
        }

        # Encode and merge payload fields
        columns_dict.update(
            {
                field: encode_to_dict(getattr(self.data, field))
                for field in self._data_model_keys
            }
        )

        return columns_dict

    @classmethod
    def create(cls, tag: str, **kwargs) -> "Message":
        """
        Factory to create a Message containing a specific ontology type.

        This method intelligently splits `kwargs` into:
        1. Envelope arguments (timestamp_ns, message_header)
        2. Payload arguments (passed to the ontology class constructor)

        Args:
            tag (str): The registered tag of the ontology data (e.g., "imu").
            **kwargs: A flat dictionary containing both message and data fields.

        Returns:
            Message: The populated message object.
        """
        # Validate Tag
        if tag not in _SENSOR_REGISTRY:
            raise ValueError(
                f"No ontology registered with tag '{tag}'. "
                f"Available tags: {list(_SENSOR_REGISTRY.keys())}"
            )

        DataClass = _SENSOR_REGISTRY[tag]

        # Cleanup Input (Fix Parquet artifacts)
        fixed_kwargs = _fix_empty_dicts(kwargs) if kwargs else dict({})
        if not fixed_kwargs:
            raise Exception(f"Unable to obtain valid fields from kwargs: {kwargs}")

        # Argument Separation
        message_fields = list(cls.model_fields.keys())
        data_fields = list(DataClass.model_fields.keys())

        # Extract Envelope args
        message_kwargs = {
            key: val
            for key, val in fixed_kwargs.items()
            if key in message_fields and key != "data"
        }
        if not message_kwargs:
            raise Exception("Input kwargs missing required Message fields.")

        # Extract Payload args
        data_kwargs = {
            key: val for key, val in fixed_kwargs.items() if key in data_fields
        }

        # Instantiation
        data_obj = DataClass(**data_kwargs)
        return cls(data=data_obj, **message_kwargs)

    @classmethod
    def get_schema(cls, data_cls: Type["Serializable"]) -> pa.Schema:
        """
        Generates the combined PyArrow Schema for a specific ontology type.

        Merges the Message envelope schema with the specific Ontology schema.

        Args:
            data_cls: The ontology class type.

        Returns:
            pa.Schema: The combined schema.
        """
        # Collision check
        colliding_keys = set(cls.__msco_pyarrow_struct__.names) & set(
            data_cls.__msco_pyarrow_struct__.names
        )
        if colliding_keys:
            raise ValueError(
                f"Class '{data_cls.__name__}' schema collides with Message schema: {list(colliding_keys)}"
            )

        return _make_schema(
            cls.__msco_pyarrow_struct__,
            data_cls.__msco_pyarrow_struct__,
        )

    def get_data(self, target_type: Type[TSensor]) -> TSensor:
        """
        Safe accessor for the data payload.

        Args:
            target_type (Type[TSensor]): The expected class of the data.

        Returns:
            TSensor: The data object, type-hinted for IDE support.

        Raises:
            TypeError: If the actual data does not match the requested type.
        """
        if not isinstance(self.data, target_type):
            raise TypeError(
                f"Message data is type '{type(self.data).__name__}', "
                f"but '{target_type.__name__}' was requested."
            )
        return self.data
