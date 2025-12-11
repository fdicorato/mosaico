"""
Covariance Mixin Module.

Similar to `HeaderMixin`, this module injects covariance fields into ontology models.
"""

from typing import List, Optional
import pyarrow as pa

from .base_model import BaseModel


class CovarianceMixin(BaseModel):
    """
    A Mixin that adds optional `covariance` and `covariance_type` fields to data models.
    Useful for sensors like IMUs or Odometry that provide uncertainty measurements.
    """

    covariance: Optional[List[float]] = None
    covariance_type: Optional[int] = None

    def __init_subclass__(cls, **kwargs):
        """
        Automatically updates the child class's PyArrow schema to include covariance fields.
        """
        super().__init_subclass__(**kwargs)

        # Define the fields to inject
        _FIELDS = [
            pa.field(
                "covariance",
                pa.list_(value_type=pa.float64()),
                nullable=True,
                metadata={
                    "description": "The covariance matrix (flattened) of the data."
                },
            ),
            pa.field(
                "covariance_type",
                pa.int16(),
                nullable=True,
                metadata={
                    "description": "Enum integer representing the covariance parameterization."
                },
            ),
        ]

        # Retrieve existing schema fields
        current_pa_fields = []
        if hasattr(cls, "__msco_pyarrow_struct__") and isinstance(
            cls.__msco_pyarrow_struct__, pa.StructType
        ):
            current_pa_fields = list(cls.__msco_pyarrow_struct__)

        # Collision Check
        existing_pa_names = [f.name for f in current_pa_fields]
        if "covariance" in existing_pa_names or "covariance_type" in existing_pa_names:
            raise ValueError(
                f"Class '{cls.__name__}' has conflicting 'covariance' or 'covariance_type' schema keys."
            )

        # Append and Update
        new_fields = current_pa_fields + _FIELDS
        cls.__msco_pyarrow_struct__ = pa.struct(new_fields)
