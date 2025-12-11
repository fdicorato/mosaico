"""
Dynamics Data Structures.

This module defines structures for forces and moments (torques).
These can be assigned to Message.data field to send data to the platform.
"""

from typing import Optional
import pyarrow as pa
from pydantic import model_validator

from ..covariance_mixin import CovarianceMixin
from ..serializable import Serializable
from ..header_mixin import HeaderMixin
from .geometry import Vector3d


class ForceTorque(
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Represents a Wrench (Force + Torque).

    Used to describe forces applied to a rigid body at a specific point.

    Components:
    - Force (Newtons): Linear force vector.
    - Torque (Newton-meters): Rotational moment vector.
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "force",
                Vector3d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "3D linear force vector"},
            ),
            pa.field(
                "torque",
                Vector3d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "3D torque vector"},
            ),
        ]
    )

    force: Optional[Vector3d] = None
    """3D linear force vector"""

    torque: Optional[Vector3d] = None
    """3D torque vector"""

    @model_validator(mode="after")
    def check_at_least_one_exists(self) -> "ForceTorque":
        """
        Validates that the object contains meaningful data.

        Raises:
            ValueError: If both `force` and `torque` are None.
        """
        if self.force is None and self.torque is None:
            raise ValueError("User must provide at least 'force' or 'torque'.")
        return self
