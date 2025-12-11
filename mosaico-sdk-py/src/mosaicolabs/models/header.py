"""
Header and Time Definitions.

This module defines the standard `Header` structure used to provide context (time, frame)
to ontology data. It includes a high-precision `Time` class to handle ROS-style
seconds/nanoseconds splitting, avoiding floating-point precision loss associated
with standard Python timestamps.
"""

from typing import Optional
import math
import time
from pydantic import field_validator
import pyarrow as pa
from datetime import datetime, timezone

from .base_model import BaseModel


class Time(BaseModel):
    """
    High-precision time representation.

    Splits time into seconds (integer) and nanoseconds (unsigned integer)
    to prevent precision loss common with 64-bit floats.
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field("sec", pa.int64()),
            pa.field("nanosec", pa.uint32()),
        ]
    )

    sec: int
    nanosec: int

    @field_validator("nanosec")
    @classmethod
    def validate_nanosec(cls, v: int) -> int:
        """Ensures nanoseconds are within the valid [0, 1e9) range."""
        if not (0 <= v < 1_000_000_000):
            raise ValueError(f"Nanoseconds must be in [0, 1e9). Got {v}")
        return v

    @classmethod
    def from_float(cls, ftime: float) -> "Time":
        """
        Factory: Creates Time from a float seconds value (e.g., `time.time()`).

        Args:
            ftime (float): Seconds since epoch.

        Returns:
            Time: The normalized sec/nanosec object.
        """
        # Handle negative timestamps (although this is assumed a wrong behavior)
        # We must account for nanoseconds to be unsigned. This must be handled by borrowing from the seconds component.
        if ftime < 0:
            # e.g. -1.5 => sec = -2
            sec = math.floor(ftime)
            # Calculate remainder to reach the next second
            nanosec = int(round((ftime - sec) * 1e9))
        else:
            sec = int(ftime)
            frac_part = ftime - sec
            # Use round() to handle floating point artifacts (e.g., 0.999999 -> 1.0)
            nanosec = int(round(frac_part * 1e9))

        # Normalize if rounding pushed nanosec to 1 second
        if nanosec >= 1_000_000_000:
            sec += 1
            nanosec = 0

        return cls(sec=sec, nanosec=nanosec)

    @classmethod
    def from_milliseconds(cls, total_milliseconds: int) -> "Time":
        """
        Factory: Creates Time from total nanoseconds.

        Args:
            total_nanoseconds (int): Total time in nanoseconds.
        """
        sec = total_milliseconds // 1_000
        nanosec = (total_milliseconds % 1_000) * 1_000_000
        return cls(sec=sec, nanosec=nanosec)

    @classmethod
    def from_nanoseconds(cls, total_nanoseconds: int) -> "Time":
        """
        Factory: Creates Time from total nanoseconds.

        Args:
            total_nanoseconds (int): Total time in nanoseconds.
        """
        sec = total_nanoseconds // 1_000_000_000
        nanosec = total_nanoseconds % 1_000_000_000
        return cls(sec=sec, nanosec=nanosec)

    @classmethod
    def from_datetime(cls, dt: datetime) -> "Time":
        """
        Factory: Creates Time from a Python datetime object.

        Args:
            dt (datetime): Date (and time).
        """
        # Note: dt.timestamp() handles timezone conversion if aware
        timestamp = dt.timestamp()
        return cls.from_float(timestamp)

    @classmethod
    def now(cls) -> "Time":
        """Factory: Returns the current system time (UTC)."""
        return cls.from_float(time.time())

    def to_float(self) -> float:
        """Converts to float seconds. Warning: Precision loss possible."""
        return float(self.sec) + float(self.nanosec) * 1e-9

    def to_nanoseconds(self) -> int:
        """Converts to total nanoseconds (integer). Preserves precision."""
        return (self.sec * 1_000_000_000) + self.nanosec

    def to_milliseconds(self) -> int:
        """Converts to total milliseconds (integer). Preserves precision."""
        return (self.sec * 1_000) + int(self.nanosec / 1_000_000)

    def to_datetime(self) -> datetime:
        """Converts to Python datetime (UTC). Warning: Precision loss (us vs ns)."""
        return datetime.fromtimestamp(self.to_float(), tz=timezone.utc)


class Header(BaseModel):
    """
    Standard metadata header for ontology data.

    Structure matches common robotics standards (like ROS):
    - Sequence ID
    - Timestamp (acquisition time)
    - Frame ID (spatial context)
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "seq",
                pa.uint32(),
                nullable=True,
                metadata={
                    "description": "Sequence ID. Legacy field, often unused in modern systems."
                },
            ),
            pa.field(
                "stamp",
                Time.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "Time of data acquisition."},
            ),
            pa.field(
                "frame_id",
                pa.string(),
                nullable=True,
                metadata={
                    "description": "Coordinate frame ID (e.g., 'map', 'camera_link' in ROS)."
                },
            ),
        ]
    )

    stamp: Time
    frame_id: str
    seq: Optional[int] = None
