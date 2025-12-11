from typing import List, Optional
import pyarrow as pa
from mosaicolabs.models import Serializable, HeaderMixin


class BatteryState(Serializable, HeaderMixin):
    """
    Represents the state of a battery power supply.

    modeled after: sensor_msgs/msg/BatteryState

    NOTE: This model is still not included in the default ontology of Mosaico and is defined specifically for the ros-bridge module
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            # --- Power Metrics ---
            pa.field("voltage", pa.float32(), metadata={"unit": "V"}),
            pa.field("temperature", pa.float32(), metadata={"unit": "C"}),
            pa.field("current", pa.float32(), metadata={"unit": "A"}),
            pa.field("charge", pa.float32(), metadata={"unit": "Ah"}),
            pa.field("capacity", pa.float32(), metadata={"unit": "Ah"}),
            pa.field("design_capacity", pa.float32(), metadata={"unit": "Ah"}),
            pa.field("percentage", pa.float32(), metadata={"range": "0-1"}),
            # --- Status & ID ---
            # Storing enums as integers to be efficient and language-agnostic
            pa.field("power_supply_status", pa.uint8()),
            pa.field("power_supply_health", pa.uint8()),
            pa.field("power_supply_technology", pa.uint8()),
            pa.field("present", pa.bool_()),
            pa.field("location", pa.string()),
            pa.field("serial_number", pa.string()),
            # --- Cell Data ---
            # Using lists for variable-length cell data
            pa.field("cell_voltage", pa.list_(pa.float32()), nullable=True),
            pa.field("cell_temperature", pa.list_(pa.float32()), nullable=True),
        ]
    )

    # Core Metrics
    voltage: float
    temperature: Optional[float]
    current: Optional[float]
    charge: Optional[float]
    capacity: Optional[float]
    design_capacity: Optional[float]
    percentage: float

    # Status
    power_supply_status: int
    power_supply_health: int
    power_supply_technology: int
    present: bool

    # Metadata
    location: str
    serial_number: str

    # Cell Details (Optional because some drivers don't report them)
    cell_voltage: List[float] = []
    cell_temperature: List[float] = []
