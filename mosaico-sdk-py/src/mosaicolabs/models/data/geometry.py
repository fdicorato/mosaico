"""
Geometry Data Structures.

This module defines vectors, points, quaternions, and spatial transforms.

* **_Struct classes**: Pure data containers inheriting only from `BaseModel`.
    They define the fields (x, y, z) and the PyArrow schema. They are used when
    embedding a vector *inside* another object (like `Transform`) to avoid
    attaching unnecessary headers/timestamps to the inner fields.
* **Public classes**: Inherit from the `_Struct`, plus `Serializable`,  `HeaderMixin`
    and 'CovarianceMixin'. These can be assigned to Message.data field
    to send data to the platform.
"""

from typing import Optional
import pyarrow as pa
from pydantic import model_validator

from ..base_model import BaseModel
from ..covariance_mixin import CovarianceMixin
from ..serializable import Serializable
from ..header_mixin import HeaderMixin


# ---------------------------------------------------------------------------
# Vector STRUCT classes
# ---------------------------------------------------------------------------


class _Vector2dStruct(BaseModel):
    """
    Internal structure for 2D vectors.
    Contains only data fields and schema, no transport logic.
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "x",
                pa.float64(),
                metadata={"description": "Vector x component"},
            ),
            pa.field(
                "y",
                pa.float64(),
                metadata={"description": "Vector y component"},
            ),
        ]
    )

    x: float
    y: float

    @classmethod
    def from_list(cls, data: list[float]):
        """
        Helper to create instance from a list.

        Args:
            data (list[float]): A list containing exactly [x, y].

        Raises:
            ValueError: If list length is not 2.
        """
        if len(data) != 2:
            raise ValueError("expected 2 values")
        return cls(x=data[0], y=data[1])


class _Vector3dStruct(BaseModel):
    """
    Internal structure for 3D vectors.
    Contains only data fields and schema, no transport logic.
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "x",
                pa.float64(),
                metadata={"description": "Vector x component"},
            ),
            pa.field(
                "y",
                pa.float64(),
                metadata={"description": "Vector y component"},
            ),
            pa.field(
                "z",
                pa.float64(),
                metadata={"description": "Vector z component"},
            ),
        ]
    )

    x: float
    y: float
    z: float

    @classmethod
    def from_list(cls, data: list[float]):
        """
        Helper to create instance from a list.

        Args:
            data (list[float]): A list containing exactly [x, y, z].

        Raises:
            ValueError: If list length is not 3.
        """
        if len(data) != 3:
            raise ValueError("expected 3 values")
        return cls(x=data[0], y=data[1], z=data[2])


class _Vector4dStruct(BaseModel):
    """
    Internal structure for 4D vectors (often used for Quaternions).
    Contains only data fields and schema, no transport logic.
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "x",
                pa.float64(),
                metadata={"description": "Vector x component"},
            ),
            pa.field(
                "y",
                pa.float64(),
                metadata={"description": "Vector y component"},
            ),
            pa.field(
                "z",
                pa.float64(),
                metadata={"description": "Vector z component"},
            ),
            pa.field(
                "w",
                pa.float64(),
                metadata={"description": "Vector w component"},
            ),
        ]
    )

    x: float
    y: float
    z: float
    w: float

    @classmethod
    def from_list(cls, data: list[float]):
        """
        Helper to create instance from a list.

        Args:
            data (list[float]): A list containing exactly [x, y, z, w].

        Raises:
            ValueError: If list length is not 4.
        """
        if len(data) != 4:
            raise ValueError("expected 4 values")
        return cls(x=data[0], y=data[1], z=data[2], w=data[3])


# ---------------------------------------------------------------------------
# Public vector classes
# ---------------------------------------------------------------------------


class Vector2d(
    _Vector2dStruct,  # Inherits fields (x, y)
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Public 2D Vector data.
    Use this class to instantiate a vector to be sent over the platform.
    """

    pass


class Vector3d(
    _Vector3dStruct,  # Inherits fields (x, y, z)
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Public 3D Vector data.
    Use this class to instantiate a vector to be sent over the platform.
    """

    pass


class Vector4d(
    _Vector4dStruct,  # Inherits fields (x, y, z, w)
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Public 4D Vector data.
    Use this class to instantiate a vector to be sent over the platform.
    """

    pass


class Point2d(
    _Vector2dStruct,  # Inherits fields (x, y)
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Semantically represents a Point in 2D space.
    Structurally identical to Vector2d but distinguished for clarity in APIs.
    """

    pass


class Point3d(
    _Vector3dStruct,  # Inherits fields (x, y, z)
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Semantically represents a Point in 3D space.
    Structurally identical to Vector3d but distinguished for clarity in APIs.
    """

    pass


class Quaternion(
    _Vector4dStruct,  # Inherits fields (x, y, z, w)
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Semantically represents a Rotation Quaternion (x, y, z, w).
    Structurally identical to Vector4d.
    """

    pass


# ---------------------------------------------------------------------------
# Composite Structures
# ---------------------------------------------------------------------------


class Transform(
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Represents a spatial transformation between two coordinate frames (Translation + Rotation).

    This is often used to describe the position of a ontology relative to the robot base,
    or the robot base relative to the world map.
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "translation",
                Vector3d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "3D translation vector"},
            ),
            pa.field(
                "rotation",
                Quaternion.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "Quaternion representing rotation."},
            ),
            pa.field(
                "target_frame_id",
                pa.string(),
                nullable=True,
                metadata={"description": "Target frame identifier."},
            ),
        ]
    )

    translation: Optional[Vector3d] = None
    """3D translation vector."""

    rotation: Optional[Quaternion] = None
    """Quaternion representing rotation."""

    target_frame_id: Optional[str] = None
    """Target frame identifier."""

    @model_validator(mode="after")
    def check_at_least_one_exists(self) -> "Transform":
        """
        Validates that the transform is not empty.

        Raises:
            ValueError: If both `translation` and `rotation` are None.
        """
        if self.translation is None and self.rotation is None:
            raise ValueError("User must provide at least 'translation' or 'rotation'.")
        return self


class Pose(
    Serializable,  # Adds Registry/Factory logic
    HeaderMixin,  # Adds Timestamp/Frame info
    CovarianceMixin,  # Adds Covariance matrix support
):
    """
    Represents the position and orientation of an object in space.
    Similar to Transform, but semantically denotes state rather than a coordinate shift.
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "position",
                Point3d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "3D translation vector"},
            ),
            pa.field(
                "orientation",
                Quaternion.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "Quaternion representing rotation."},
            ),
        ]
    )

    position: Optional[Point3d] = None
    """3D translation vector"""

    orientation: Optional[Quaternion] = None
    """Quaternion representing rotation."""

    @model_validator(mode="after")
    def check_at_least_one_exists(self) -> "Pose":
        """
        Validates that the Pose is not empty.

        Raises:
            ValueError: If both `position` and `orientation` are None.
        """
        if self.position is None and self.orientation is None:
            raise ValueError("User must provide at least 'position' or 'orientation'.")
        return self
