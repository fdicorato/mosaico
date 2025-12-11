import pyarrow as pa
from typing import List
from mosaicolabs.models.data import Transform
from mosaicolabs.models import Serializable, HeaderMixin


class FrameTransform(Serializable, HeaderMixin):
    """
    NOTE: This model is not included in the default ontology of Mosaico and is defined specifically for the ros-bridge module
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "transforms",
                pa.list_(value_type=Transform.__msco_pyarrow_struct__),
                nullable=False,
                metadata={"description": "List of coordinate frames transformations."},
            ),
        ]
    )

    transforms: List[Transform]
