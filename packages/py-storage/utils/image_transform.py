from __future__ import annotations

import io
import logging
from pathlib import Path

from PIL import Image, ImageOps

from ecom_storage.configs.minio_config import (
    ALLOWED_IMAGE_EXTENSIONS,
    IMAGE_VARIANT_SIZES,
    JPG_QUALITY,
    ImageVariant,
)
from utils.exceptions import MinioOperationError

try:
    from ecom_shared.logging import get_logger
    logger = get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_image_file(file_path: str | Path) -> None:
    """
    Checks only the file extension (fast, no I/O beyond stat). Actual
    image integrity is verified by Pillow when the file is opened.
    """
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Image file not found: {path}")

    ext = path.suffix.lower()
    if ext not in ALLOWED_IMAGE_EXTENSIONS:
        raise MinioOperationError(
            "validate_image",
            f"Unsupported image format '{ext}'. "
            f"Allowed extensions: {sorted(ALLOWED_IMAGE_EXTENSIONS)}",
        )


def process_image(
    file_path: str | Path,
    variant: ImageVariant,
) -> bytes:
    """
    Load *file_path*, apply the variant transform, and return JPEG bytes.

    Steps
    -----
    1. Open with Pillow and auto-apply EXIF orientation.
    2. Convert to RGB (strips alpha — JPEG does not support transparency).
    3. Apply resize/crop for the given variant.
    4. Encode to JPEG at the quality defined in ``JPG_QUALITY``.
    """
    path = Path(file_path)
    try:
        with Image.open(path) as img:
            img = ImageOps.exif_transpose(img)
            img = _convert_to_rgb(img)
            img = _apply_variant(img, variant)
            return _encode_jpg(img, variant)
    except OSError as exc:
        raise MinioOperationError(
            "process_image",
            f"Cannot open image '{path}': {exc}",
            exc,
        ) from exc


def process_all_variants(
    file_path: str | Path,
) -> dict[ImageVariant, bytes]:
    return {variant: process_image(file_path, variant) for variant in ImageVariant}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _convert_to_rgb(img: Image.Image) -> Image.Image:
    """Convert image to RGB, compositing transparency on a white background."""
    if img.mode in ("RGBA", "LA"):
        background = Image.new("RGB", img.size, (255, 255, 255))
        mask = img.split()[-1]  # alpha channel
        background.paste(img.convert("RGB"), mask=mask)
        return background
    if img.mode != "RGB":
        return img.convert("RGB")
    return img


def _apply_variant(img: Image.Image, variant: ImageVariant) -> Image.Image:
    """Apply the resize/crop strategy for the given variant."""
    size = IMAGE_VARIANT_SIZES[variant]

    if size is None:
        return img

    max_w, max_h = size

    if variant == ImageVariant.THUMBNAIL:
        return _center_crop(img, max_w, max_h)

    # large — fit inside box, preserve aspect ratio
    return _fit_inside(img, max_w, max_h)


def _fit_inside(img: Image.Image, max_w: int, max_h: int) -> Image.Image:
    """Scale down proportionally so the image fits inside (max_w × max_h)."""
    img.thumbnail((max_w, max_h), Image.Resampling.LANCZOS)
    return img


def _center_crop(img: Image.Image, target_w: int, target_h: int) -> Image.Image:
    """
    Resize + center-crop to exactly (target_w × target_h).

    Strategy:
        1. Scale so the shorter side equals the target dimension.
        2. Crop the centre to the exact target size.
    """
    src_w, src_h = img.size
    scale = max(target_w / src_w, target_h / src_h)
    new_w = max(target_w, int(src_w * scale))
    new_h = max(target_h, int(src_h * scale))
    img = img.resize((new_w, new_h), Image.Resampling.LANCZOS)

    left = (new_w - target_w) // 2
    top  = (new_h - target_h) // 2
    return img.crop((left, top, left + target_w, top + target_h))


def _encode_jpg(img: Image.Image, variant: ImageVariant) -> bytes:
    """Encode *img* to JPEG bytes at the quality configured for *variant*."""
    buf = io.BytesIO()
    img.save(
        buf,
        format="JPEG",
        quality=JPG_QUALITY[variant],
        optimize=True,
        progressive=True,
    )
    return buf.getvalue()
