from abc import ABC, abstractmethod
from typing import Generic, Optional, Tuple, Type, Any, TypeVar

from mosaicolabs.models.message import Message

from .ros_message import ROSMessage
from ..models import Serializable

T = TypeVar("T", bound=Serializable)


class ROSAdapterBase(ABC, Generic[T]):
    """
    Abstract Base Class for converting a ROS message to an Ontology Ontology Data type.
    """

    ros_msgtype: str | Tuple[str, ...]
    __mosaico_ontology_type__: Type[T]
    _REQUIRED_KEYS: Tuple[str, ...]
    _REQUIRED_KEYS_CASE_INSENSITIVE: Tuple[str, ...] = ()

    @classmethod
    @abstractmethod
    def ros_msg_type(cls) -> str | Tuple[str, ...]:
        """Returns the specific ROS message type handled by this adapter."""
        return cls.ros_msgtype

    @classmethod
    @abstractmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS message instance into an Ontology ontology data instance.
        """
        pass

    @classmethod
    @abstractmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        pass

    @classmethod
    def ontology_data_type(cls) -> Type[T]:
        """Returns the Ontology class type associated with this adapter."""
        return cls.__mosaico_ontology_type__
