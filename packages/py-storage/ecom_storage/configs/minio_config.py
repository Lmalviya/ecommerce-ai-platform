from __future__ import annotations

import os
from datetime import timedelta
from enum import Enum

# ---------------------------------------------------------------------------
# Connection settings (raw reads — validated below)
# ---------------------------------------------------------------------------
MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ROOT_USER", "")
MINIO_SECRET_KEY: str = os.getenv("MINIO_ROOT_PASSWORD", "")
MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"

# ---------------------------------------------------------------------------
# Fail-fast validation — runs once at import / service startup
# ---------------------------------------------------------------------------
_REQUIRED: dict[str, str] = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ROOT_USER": MINIO_ACCESS_KEY,
    "MINIO_ROOT_PASSWORD": MINIO_SECRET_KEY,
}

_missing = [name for name, value in _REQUIRED.items() if not value]

if _missing:
    raise EnvironmentError(
        f"Missing required MinIO environment variable(s): {', '.join(_missing)}. "
        "Set them in your .env file or container environment before starting the service."
    )

# ---------------------------------------------------------------------------
# Bucket names (hardcoded — these are infrastructure constants, not secrets)
# ---------------------------------------------------------------------------
BUCKET_UPLOADS_TEMP: str = os.getenv("IMAGE_UPLOAD_TEMP_BUCKET", "uploads-temp")
BUCKET_PRODUCTS: str = os.getenv("IMAGE_UPLOAD_PRODUCT_BUCKET", "products")         

# ---------------------------------------------------------------------------
# Presigned URL config
# ---------------------------------------------------------------------------
# uploads-temp uses presigned URLs (bucket is private)
TEMP_URL_EXPIRY: timedelta = timedelta(hours=int(os.getenv("TEMP_URL_EXPIRY_TIME", 24)))

# ---------------------------------------------------------------------------
# Image variant definitions
# ---------------------------------------------------------------------------
class ImageVariant(str, Enum):
    """
    Supported image size variants stored under the products bucket.

    Variant     Max dimensions  Resize strategy
    ----------  ---------------  ---------------
    original    No resize        JPEG conversion only (master copy)
    large       800 × 800        Fit inside box, preserve aspect ratio
    thumbnail   150 × 150        Center-crop to exact square
    """
    ORIGINAL  = "original"
    LARGE     = "large"
    THUMBNAIL = "thumbnail"


# Mapping: variant → (width, height) or None for no resize
IMAGE_VARIANT_SIZES: dict[ImageVariant, tuple[int, int] | None] = {
    ImageVariant.ORIGINAL:  None,           # convert to JPEG only
    ImageVariant.LARGE:     (800, 800),     # fit, preserve aspect ratio
    ImageVariant.THUMBNAIL: (150, 150),     # center-crop square
}

# Valid input image extensions accepted for upload
ALLOWED_IMAGE_EXTENSIONS: frozenset[str] = frozenset({
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".tif",
})

# All processed images are stored as JPEG
OUTPUT_IMAGE_CONTENT_TYPE: str = "image/jpeg"
OUTPUT_IMAGE_EXTENSION: str = ".jpg"

# JPEG quality for each variant (0–95)
JPG_QUALITY: dict[ImageVariant, int] = {
    ImageVariant.ORIGINAL:  92,
    ImageVariant.LARGE:     88,
    ImageVariant.THUMBNAIL: 85,
}
