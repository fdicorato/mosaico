from dataclasses import dataclass
from typing import Any, Dict, Optional

from mosaicolabs.models import Header, Time


def _validate_header_fields(ros_hdata: dict):
    if (
        "frame_id" not in ros_hdata.keys()
        or "stamp" not in ros_hdata.keys()
        or (
            "sec" not in ros_hdata["stamp"].keys()
            or "nanosec" not in ros_hdata["stamp"].keys()
        )
    ):
        raise ValueError(
            f"Malformed ROS message header: missing required keys 'frame_id', 'stamp' or 'sec', 'nanosec' in 'stamp'. "
            f"Available keys: {list(ros_hdata.keys())}"
        )


@dataclass
class ROSHeader:
    seq: Optional[int]
    """sequence ID: consecutively increasing ID """
    frame_id: str
    """Frame this data is associated with"""
    stamp: Time
    """seconds (stamp_secs) since epoch (in Python the variable is called 'secs')"""

    _REQUIRED_KEYS = ("frame_id", "stamp")

    def translate(
        self,
        **kwargs: Any,
    ) -> Header:
        # --- Extract Metadata ---
        return Header(
            frame_id=self.frame_id,
            seq=self.seq,
            stamp=self.stamp,
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ROSHeader":
        _validate_header_fields(data)
        return ROSHeader(
            seq=data.get("seq"),
            frame_id=data["frame_id"],
            stamp=Time(sec=data["stamp"]["sec"], nanosec=data["stamp"]["nanosec"]),
        )


@dataclass
class ROSMessage:
    """
    The standardized container for a single ROS message record yielded by the loader.
    """

    def __init__(
        self, timestamp: int, topic: str, msg_type: str, data: Optional[Dict[str, Any]]
    ):
        self.timestamp = timestamp
        self.topic = topic
        self.msg_type = msg_type
        self.data = data
        if data:
            header_dict = data.get("header")
            if header_dict:
                self.header = ROSHeader.from_dict(header_dict)

    timestamp: int
    """The nanosecond-precise timestamp of the message."""
    topic: str
    """The topic string of the message source."""
    msg_type: str
    """The message ros type string."""
    data: Optional[Dict[str, Any]]
    """The message payload, converted into a standard nested Python dictionary."""
    header: Optional[ROSHeader] = None
