"""
Image Transport Module.

This module handles the serialization and deserialization of image data.
It provides:
1.  **Raw Image Handling (`Image`)**: Manages uncompressed pixel data with explicit
    control over memory strides, endianness, and pixel formats (encoding).
2.  **Compressed Image Handling (`CompressedImage`)**: Manages encoded data streams
    (JPEG, PNG, H.264) using a pluggable codec architecture.
"""

from abc import ABC, abstractmethod
from enum import Enum
import logging as log
import io
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

# dependencies for video handling
import av
import numpy as np
import pyarrow as pa
from PIL import Image as PILImage

from mosaicolabs.enum import SerializationFormat

from ..header import Header
from ..header_mixin import HeaderMixin
from ..serializable import Serializable


class ImageFormat(str, Enum):
    """Supported containers for image formats."""

    RAW = "raw"
    PNG = "png"
    JPEG = "jpeg"
    TIFF = "tiff"
    H264 = "h264"
    HEVC = "hevc"


# Configuration mapping string encodings (transport layer) to Python types (application layer).
# Format: "encoding_name": (NumPy_Dtype, Channel_Count, PIL_Mode)
_IMG_ENCODING_MAP: dict = {
    # 8-bit Color
    "rgb8": (np.uint8, 3, "RGB"),
    "bgr8": (np.uint8, 3, "RGB"),  # Requires BGR->RGB Swap for PIL
    "rgba8": (np.uint8, 4, "RGBA"),
    "bgra8": (np.uint8, 4, "RGBA"),  # Requires BGRA->RGBA Swap for PIL
    "mono8": (np.uint8, 1, "L"),
    "8UC1": (np.uint8, 1, "L"),
    "8UC3": (np.uint8, 3, "RGB"),
    "8UC4": (np.uint8, 4, "RGBA"),
    # --- Bayer (Treat as Grayscale "L" to preserve raw mosaic pattern) ---
    "bayer_rggb8": (np.uint8, 1, "L"),
    "bayer_bggr8": (np.uint8, 1, "L"),
    "bayer_gbrg8": (np.uint8, 1, "L"),
    "bayer_grbg8": (np.uint8, 1, "L"),
    # --- 16-bit Unsigned (Depth/IR) ---
    "mono16": (np.uint16, 1, "I;16"),
    "rgb16": (np.uint16, 3, "RGB"),
    "bgr16": (np.uint16, 3, "RGB"),
    "rgba16": (np.uint16, 4, "RGBA"),
    "bgra16": (np.uint16, 4, "RGBA"),
    "16UC1": (np.uint16, 1, "I;16"),
    "16UC3": (np.uint16, 3, "RGB"),
    "16UC4": (np.uint16, 4, "RGBA"),
    # --- 16-bit Signed ---
    "16SC1": (np.int16, 1, "I"),
    # --- 32-bit Integer ---
    "32SC1": (np.int32, 1, "I"),
    # --- 32-bit Float ---
    "32FC1": (np.float32, 1, "F"),
    # --- RAW Fallback ---
    "8UC2": (np.uint8, 2, None),
    "16UC2": (np.uint16, 2, None),
    "32FC2": (np.float32, 2, None),
    "32FC3": (np.float32, 3, None),
    "32FC4": (np.float32, 4, None),
    "64FC1": (np.float64, 1, None),
}

_DEFAULT_IMG_FORMAT = ImageFormat.PNG


class Image(Serializable, HeaderMixin):
    """
    Represents raw, uncompressed image data.

    This class provides a flattened, row-major binary representation of an image.
    It is designed to handle:
    1.  **Arbitrary Data Types**: From standard uint8 RGB to float32 Depth and uint16 IR.
    2.  **Memory Layouts**: Explicit control over `stride` (stride) and endianness (`is_bigendian`).
    3.  **Transport**: Can act as a container for RAW bytes or wrap them in lossless containers (PNG).
    """

    # --- Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.binary(),
                nullable=False,
                metadata={"description": "The flattened image memory buffer."},
            ),
            pa.field(
                "format",
                pa.string(),
                nullable=False,
                metadata={"description": "Container format (e.g., 'raw', 'png')."},
            ),
            pa.field(
                "width",
                pa.int32(),
                nullable=False,
                metadata={"description": "Image width in pixels."},
            ),
            pa.field(
                "height",
                pa.int32(),
                nullable=False,
                metadata={"description": "Image height in pixels."},
            ),
            pa.field(
                "stride",
                pa.int32(),
                nullable=True,
                metadata={"description": "Bytes per row. Essential for alignment."},
            ),
            pa.field(
                "is_bigendian",
                pa.bool_(),
                nullable=True,
                metadata={
                    "description": "True if data is Big-Endian. Defaults to system endianness if null."
                },
            ),
            pa.field(
                "encoding",
                pa.string(),
                nullable=True,
                metadata={"description": "Pixel format (e.g., 'bgr8', 'mono16')."},
            ),
        ]
    )

    # Sets the batching strategy to 'Bytes' (instead of 'Count') for better network performance
    __serialization_format__ = SerializationFormat.Image
    __supported_image_formats__ = [ImageFormat.PNG, ImageFormat.RAW]

    # Pydantic Fields
    data: bytes
    """The flattened image memory buffer."""

    format: ImageFormat
    """The format used for serialization ('png' or 'raw')."""

    width: int
    """The width of the image in pixels."""

    height: int
    """The height of the image in pixels."""

    stride: int
    """The number of bytes per row of the image."""

    encoding: str
    """The pixel encoding (e.g., 'bgr8', 'mono16'). Optional field."""

    is_bigendian: Optional[bool] = None
    """Store if the original data is Big-Endian. Optional field."""

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        if self.format not in self.__supported_image_formats__:
            raise ValueError(
                f"Invalid image format {self.format}. Supported formats {self.__supported_image_formats__}"
            )

    @classmethod
    def encode(
        cls,
        data: List[int],
        stride: int,
        height: int,
        width: int,
        encoding: str,
        header: Optional[Header] = None,
        is_bigendian: Optional[bool] = None,
        format: Optional[ImageFormat] = _DEFAULT_IMG_FORMAT,
    ) -> "Image":
        """
        Encodes linear pixel uint8 data into the storage container.

        **The "Wide Grayscale" Trick:**
        When saving complex types (like `float32` depth or `uint16` raw) into standard
        image containers like PNG, we cannot rely on standard RGB encoders as they might
        apply color corrections or bit-depth reductions.

        Instead, this method treats the data as a raw byte stream. It reshapes the
        stream into a 2D "Grayscale" image where:
        - `Image_Height` = `Original_Height`
        - `Image_Width` = `Stride` (The full row stride in bytes)

        This guarantees that every bit of the original memory (including padding) is
        preserved losslessly.

        Args:
            data (List[int]): Flattened list of bytes (uint8).
            stride (int): Row stride in bytes.
            height (int): Image height.
            width (int): Image width.
            encoding (str): Pixel format string.
            format (ImageFormat): Target container ('raw', 'png', etc.).

        Returns:
            Image: An instantiated object.
        """
        if not format:
            format = _DEFAULT_IMG_FORMAT

        raw_bytes = bytes(data)

        if format == ImageFormat.RAW:
            img_bytes = raw_bytes
        else:
            try:
                # View as uint8
                arr_uint8 = np.frombuffer(raw_bytes, dtype=np.uint8)

                # Reshape based on physical memory layout (Height x Stride)
                # ignoring logical width to preserve padding.
                matrix_shape = (height, stride)
                arr_reshaped = arr_uint8.reshape(matrix_shape)

                # Save as Mode 'L' (8-bit grayscale)
                pil_image = PILImage.fromarray(
                    arr_reshaped
                )  # avoid mode ='L' because is deprecated
                buf = io.BytesIO()
                pil_image.save(buf, format=format.value.upper())
                img_bytes = buf.getvalue()

            except Exception as e:
                log.error(f"Encoding failed ({e}). Falling back to RAW.")
                img_bytes = raw_bytes
                format = ImageFormat.RAW

        return cls(
            header=header,
            data=img_bytes,
            format=format,
            width=width,
            height=height,
            stride=stride,
            is_bigendian=is_bigendian,
            encoding=encoding,
        )

    def decode(self) -> List[int]:
        """
        Decodes the storage container back to a linear byte list.

        Reverses the "Wide Grayscale" encoding to return the original,
        flattened memory buffer.

        Returns:
            List[int]: A list of uint8 integers representing the raw memory.
        """
        if self.format == ImageFormat.RAW:
            return list(self.data)

        try:
            # PIL reads the header to get dimensions (Height, Step)
            with PILImage.open(io.BytesIO(self.data)) as img:
                arr_uint8 = np.array(img)

            # Flatten back to 1D
            raw_bytes = arr_uint8.tobytes()
            return list(raw_bytes)

        except Exception:
            return list(self.data)

    def to_pillow(self) -> PILImage.Image:
        """
        Converts the raw binary data into a standard PIL Image.

        This method performs the heavy lifting of interpretation:
        1.  **Decoding**: Unpacks the transport container (e.g., PNG -> bytes).
        2.  **Casting**: Interprets bytes according to `self.encoding` (e.g., as float32).
        3.  **Endianness**: Swaps bytes if the source endianness differs from the local CPU.
        4.  **Color Swap**: Converts BGR (common in OpenCV/Robotics) to RGB (required by PIL).

        Returns:
            PILImage.Image: A visualizable image object.

        Raises:
            NotImplementedError: If the encoding is unknown.
            ValueError: If data size doesn't match dimensions.
        """
        if self.encoding not in _IMG_ENCODING_MAP:
            raise NotImplementedError(
                f"Encoding '{self.encoding}' not supported for PIL conversion."
            )

        dtype, channels, mode = _IMG_ENCODING_MAP[self.encoding]
        decoded_uint8_list = self.decode()
        raw_bytes = bytes(decoded_uint8_list)

        # Attempt to interpret raw_bytes with the given dtype. If any error
        # (like the data cannot be evenly divided into the required number of elements)
        # convert to uint8 and then interpret (view) as dtype
        try:
            arr = np.frombuffer(raw_bytes, dtype=dtype)
        except ValueError:
            # here we only need memory contiguity
            arr = np.frombuffer(raw_bytes, dtype=np.uint8).view(dtype)

        # Handle Endianness
        system_is_little = sys.byteorder == "little"
        source_is_big = (
            self.is_bigendian
            if self.is_bigendian is not None
            else (not system_is_little)
        )

        if (source_is_big and system_is_little) or (
            not source_is_big and not system_is_little
        ):
            arr = arr.byteswap()

        # Reshape and Validate
        expected_items = self.width * self.height * channels
        if arr.size != expected_items:
            # Handle strided padding by truncation
            if arr.size > expected_items:
                arr = arr[:expected_items]
            else:
                raise ValueError(
                    f"Data size mismatch. Expected {expected_items}, got {arr.size}"
                )

        shape = (
            (self.height, self.width, channels)
            if channels > 1
            else (self.height, self.width)
        )
        arr = arr.reshape(shape)

        # Handle BGR -> RGB
        if self.encoding in ["bgr8", "bgra8", "bgr16", "bgra16"]:
            arr = arr[..., ::-1]

        return PILImage.fromarray(arr, mode=mode)

    @classmethod
    def from_pillow(
        cls,
        pil_image: PILImage.Image,
        header: Optional[Header] = None,
        target_encoding: Optional[str] = None,
        output_format: Optional[ImageFormat] = None,
    ) -> "Image":
        """
        Factory method to create an Image from a PIL object.

        Automatically handles:
        - Data flattening (row-major).
        - Stride calculation.
        - RGB to BGR conversion (if target_encoding requires it).
        - Type casting (e.g., float -> uint8).

        Args:
            pil_image (PILImage.Image): Source image.
            header (Optional[Header]): Metadata.
            target_encoding (Optional[str]): Target pixel format (e.g., "bgr8").
            output_format (Optional[ImageFormat]): Storage container (e.g., PNG).

        Returns:
            Image: Populated data object.
        """
        arr = np.array(pil_image)

        # Default encoding inference
        if target_encoding is None:
            mode_map = {
                "L": "mono8",
                "RGB": "rgb8",
                "RGBA": "rgba8",
                "F": "32FC1",
                "I;16": "mono16",
            }
            target_encoding = mode_map.get(pil_image.mode, "rgb8")

        expected_dtype, _, _ = _IMG_ENCODING_MAP.get(
            target_encoding, (np.uint8, 1, None)
        )

        # Enforce Type
        if arr.dtype != expected_dtype:
            arr = arr.astype(expected_dtype)

        # Handle RGB -> BGR
        if target_encoding in ["bgr8", "bgra8", "bgr16", "bgra16"]:
            if arr.ndim == 3:
                arr = arr[..., ::-1]

        # Ensure contiguous memory for correct stride calc
        arr = np.ascontiguousarray(arr)

        raw_bytes = arr.tobytes()
        data_list = list(raw_bytes)
        stride = arr.strides[0]
        height, width = arr.shape[:2]

        return cls.encode(
            data=data_list,
            stride=stride,
            width=width,
            format=output_format,
            encoding=target_encoding,
            height=height,
            is_bigendian=sys.byteorder == "big",
            header=header,
        )


class CompressedImageCodec(ABC):
    """
    Abstract Strategy interface for encoding and decoding compressed binary image data.

    This class defines the contract for handling various compression formats.
    Subclasses can implement support for various standard images (JPEG, PNG),
    video frames (H.264, HEVC), or others.

    Attributes:
        __available_formats__ (Tuple[str, ...]): A tuple of lowercase format strings
            supported by this codec (e.g., ("jpeg", "png")).
    """

    __available_formats__: Tuple[str, ...] = ()

    @abstractmethod
    def decode(
        self, data_bytes: bytes, format: ImageFormat
    ) -> Optional[PILImage.Image]:
        """
        Decodes raw binary bytes into a usable Image object.

        Args:
            data: The raw byte stream of the compressed image/video frame.
            format: The format string identifier (e.g., 'jpeg', 'h264').

        Returns:
            Optional[TImage]: The decoded image object (e.g. PIL.Image),
            or None if decoding fails.
        """
        pass

    @abstractmethod
    def encode(self, image: PILImage.Image, format: ImageFormat, **kwargs) -> bytes:
        """
        Encodes an Image object into compressed raw binary bytes.

        Args:
            image: The source image object.
            format: The target compression format.
            **kwargs: Format-specific parameters (e.g., quality=85).

        Returns:
            bytes: The compressed binary data.

        Raises:
            ValueError: If the format is not supported for encoding.
        """
        pass


# --- Registry System ---

_IMG_CODECS_FACTORY: Dict[str, CompressedImageCodec] = {}


def register_codec(formats: Iterable[ImageFormat]):
    """Decorator to register a codec for specific formats (jpeg, h264, etc.)."""

    def decorator(cls):
        # This ensures self.__available_formats__ is available at runtime
        # without needing to define it manually in the class body.
        cls.__available_formats__ = tuple(formats)
        instance = cls()
        for fmt in formats:
            normalized_fmt = fmt.value.lower()
            if normalized_fmt in _IMG_CODECS_FACTORY:
                log.warning(
                    f"Warning: Format '{normalized_fmt}' already assigned to "
                    f"{type(_IMG_CODECS_FACTORY[normalized_fmt]).__name__} "
                    "and will be overwritten"
                )
            _IMG_CODECS_FACTORY[normalized_fmt] = instance
        return cls

    return decorator


@register_codec([ImageFormat.JPEG, ImageFormat.PNG, ImageFormat.TIFF])
class DefaultCodec(CompressedImageCodec):
    """
    Standard codec implementation using the Pillow (PIL) library.
    Handles common image formats like JPEG, PNG, and TIFF.
    """

    def decode(
        self, data_bytes: bytes, format: ImageFormat
    ) -> Optional[PILImage.Image]:
        """Decodes bytes using PIL.Image.open."""
        try:
            image = PILImage.open(io.BytesIO(data_bytes))
            image.load()
            return image
        except Exception as e:
            log.error(f"DefaultCodec decode error: {e}")
            return None

    def encode(self, image: PILImage.Image, format: ImageFormat, **kwargs) -> bytes:
        """Encodes image using PIL.Image.save."""
        buf = io.BytesIO()
        try:
            image.save(buf, format=format.value.upper(), **kwargs)
            return buf.getvalue()
        except Exception as e:
            log.error(f"DefaultCodec encode error: {e}")
            raise ValueError(f"Encoding failed: {e}")


@register_codec([ImageFormat.H264, ImageFormat.HEVC])
class VideoAwareCodec(DefaultCodec):
    """
    Advanced codec that uses PyAV to decode video frames if available,
    falling back to DefaultCodec logic for unsupported formats.
    """

    # Store decoder contexts for each video format (e.g., {"h264": <Context>, "hevc": <Context>})
    _decoder_contexts: Dict[str, av.CodecContext] = {}

    def __init__(self):
        super().__init__()
        # Initialize the cache for decoder contexts
        self._decoder_contexts = {}

    def _get_or_create_decoder(
        self, format_name: ImageFormat
    ) -> Optional[av.CodecContext]:
        """Gets a cached decoder context or creates a new one for the specified format."""

        format_key = format_name.value.lower()
        # Check if context is already cached
        if format_key in self._decoder_contexts:
            return self._decoder_contexts[format_key]
        # Create and cache a new decoder context
        try:
            # 'r' mode for reading (decoding)
            decoder = av.CodecContext.create(format_key, "r")
            self._decoder_contexts[format_key] = decoder
            log.debug(f"Created new PyAV decoder for format: {format_name}")
            return decoder
        except Exception as e:
            log.error(
                f"Failed to create PyAV decoder for {format_name} ({format_key}): {e}"
            )
            return None

    def decode(
        self, data_bytes: bytes, format: ImageFormat
    ) -> Optional[PILImage.Image]:
        # --- Handle Video Formats ---
        if format.lower() in self.__available_formats__:
            decoder = self._get_or_create_decoder(format)
            if decoder:
                try:
                    # Create packet from raw data
                    packet = av.Packet(data_bytes)

                    # Decode the packet (may yield 0 or more frames)
                    for frame in decoder.decode(packet):
                        # Convert PyAV frame (RGB) to NumPy array
                        img_np_rgb = frame.to_rgb().to_ndarray()

                        # We must ensure the NumPy array passed to fromarray is RGB.

                        # Use the RGB NumPy array directly for PIL conversion
                        return PILImage.fromarray(img_np_rgb)

                    # If decode yielded no frames (e.g., waiting for keyframe), continue.
                    log.debug(
                        f"PyAV decode yielded no frame for {format}. Waiting for keyframe."
                    )
                    return None

                except Exception as e:
                    # Catch PyAV-specific or numpy conversion errors
                    log.error(f"PyAV decode error for {format}: {e}")
                    return None
            else:
                log.warning(
                    f"Could not get decoder for video format: {format}. Dropping message."
                )
                return None

        # --- Fallback to DefaultCodec (e.g., JPEG, PNG) ---
        else:
            return super().decode(data_bytes, format)

    def encode(self, image: PILImage.Image, format: ImageFormat, **kwargs) -> bytes:
        """
        Not yet implemented.

        Raises NotImplementedError exception.
        """

        raise NotImplementedError(
            f"Direct encoding of single images to video format '{format}' "
            "is not currently supported. Consider saving as JPEG."
        )


# --- Data Structure ---


class CompressedImage(Serializable, HeaderMixin):
    """
    Represents image data stored as a compressed binary blob (e.g. JPEG, PNG, H264, ...).

    This class acts as a data container. It delegates the complex logic of
    decoding (bytes -> Image) and encoding (Image -> bytes) to the registered
    codecs in `_IMG_CODECS_FACTORY`.

    Attributes:
        data (bytes): The compressed binary payload.
        format (str): The format identifier string (e.g., 'jpeg', 'png').
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.binary(),
                nullable=False,
                metadata={
                    "description": "The serialized (compressed) image data as bytes."
                },
            ),
            pa.field(
                "format",
                pa.string(),
                nullable=False,
                metadata={
                    "description": "The compression format (e.g., 'jpeg', 'png')."
                },
            ),
        ]
    )

    __serialization_format__ = SerializationFormat.Image

    data: bytes
    """The serialized (compressed) image data as bytes."""

    format: ImageFormat
    """The compression format (e.g., 'jpeg', 'png')."""

    def to_image(
        self, codec: Optional[CompressedImageCodec] = None
    ) -> Optional[PILImage.Image]:
        """
        Decompresses the stored binary data into a usable PIL Image object.

        Args:
            codec: An optional specific codec instance to use. If None, the
                   appropriate codec is resolved automatically based on `self.format`.

        Returns:
            PILImage.Image: A ready-to-use Pillow image object.
            None: If the data is empty or decoding fails.
        """
        if not self.data:
            return None
        _codec = codec or _IMG_CODECS_FACTORY.get(self.format.value.lower())
        if not _codec:
            log.error(
                f"No codec found for format '{self.format.value}'. "
                f"Available: {list(_IMG_CODECS_FACTORY.keys())}"
            )
            return None
        return _codec.decode(self.data, self.format)

    @classmethod
    def from_image(
        cls,
        image: PILImage.Image,
        format: ImageFormat = ImageFormat.PNG,
        header: Optional[Header] = None,
        codec: Optional[CompressedImageCodec] = None,
        **kwargs,
    ) -> "CompressedImage":
        """
        Factory method to create a CompressedImage from a PIL Image.

        Args:
            image: The source Pillow image.
            format: The target compression format (default: 'jpeg').
            header: Optional Header metadata.
            codec: Optional specific codec to use.
            **kwargs: Additional arguments passed to the codec's encode method
                      (e.g., quality=90).

        Returns:
            CompressedImage: A new instance containing the compressed bytes.

        Raises:
            ValueError: If no codec is found or encoding fails.
        """
        fmt_lower = format.value.lower()
        _codec = codec or _IMG_CODECS_FACTORY.get(fmt_lower)
        if not _codec:
            raise ValueError(
                f"No codec found for format '{fmt_lower}'. "
                f"Available: {list(_IMG_CODECS_FACTORY.keys())}"
            )

        try:
            compressed_bytes = _codec.encode(image, format, **kwargs)
            return cls(data=compressed_bytes, format=format, header=header)
        except Exception as e:
            raise RuntimeError(
                f"Failed to create CompressedImage (format: {fmt_lower}): {e}"
            )
