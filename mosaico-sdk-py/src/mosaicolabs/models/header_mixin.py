"""
Header Mixin Module.

This module provides `HeaderMixin`, a helper class used to inject standard
header fields into ontology models via composition.
"""

from typing import Optional
import pyarrow as pa

from .base_model import BaseModel
from .header import Header


class HeaderMixin(BaseModel):
    """
    A Mixin that adds a `header` field to any data model inheriting from it.

    **Mechanism:**
    It uses the `__init_subclass__` hook to inspect the child class's existing
    PyArrow struct, appends the 'header' field definition, and updates the struct.
    This ensures that the PyArrow schema matches the Pydantic fields.
    """

    header: Optional[Header] = None

    def __init_subclass__(cls, **kwargs):
        """
        Automatically updates the child class's PyArrow schema to include 'header'.
        """
        super().__init_subclass__(**kwargs)

        # Define the PyArrow field definition for the header
        _HEADER_FIELD = pa.field(
            "header",
            Header.__msco_pyarrow_struct__,
            nullable=True,
            metadata={"description": "The standard metadata header (optional)."},
        )

        # Retrieve existing schema fields from the child class
        current_pa_fields = []
        if hasattr(cls, "__msco_pyarrow_struct__") and isinstance(
            cls.__msco_pyarrow_struct__, pa.StructType
        ):
            current_pa_fields = list(cls.__msco_pyarrow_struct__)

        # Collision Check
        existing_pa_names = [f.name for f in current_pa_fields]
        if "header" in existing_pa_names:
            raise ValueError(
                f"Class '{cls.__name__}' has conflicting 'header' schema key."
            )

        # Append and Update
        new_fields = current_pa_fields + [_HEADER_FIELD]
        cls.__msco_pyarrow_struct__ = pa.struct(new_fields)
