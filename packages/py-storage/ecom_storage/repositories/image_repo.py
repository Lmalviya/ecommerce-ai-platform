from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path

from minio.error import S3Error

from ecom_storage.clients.minio_client import MinioClient
from ecom_storage.configs.minio_config import (
    BUCKET_PRODUCTS,
    BUCKET_UPLOADS_TEMP,
    TEMP_URL_EXPIRY,
    ImageVariant,
)
from utils.exceptions import MinioOperationError
from utils.image_transform import process_all_variants, validate_image_file
from utils.minio_helper import (
    _delete_keys,
    _download_bytes,
    _product_key,
    _upload_bytes_to_products,
)

try:
    from ecom_shared.logging import get_logger
    logger = get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Return type for product image uploads
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ProductImageURLs:
    """
    Public URLs for all three variants of an uploaded product image.

    Attributes
    ----------
    original:
        Full-resolution JPEG master.
    large:
        800 × 800 max, aspect-ratio-preserving.
    thumbnail:
        150 × 150 center-cropped square.
    item_id:
        The item identifier used to construct the object keys.
    category:
        The product category used as the first path segment.

    Example URLs
    ------------
    original  → http://localhost:9000/products/electronics/original/B07.jpg
    large     → http://localhost:9000/products/electronics/large/B07.jpg
    thumbnail → http://localhost:9000/products/electronics/thumbnail/B07.jpg
    """
    original: str
    large: str
    thumbnail: str
    item_id: str
    category: str

    def as_dict(self) -> dict[str, str]:
        return {
            "original":  self.original,
            "large":     self.large,
            "thumbnail": self.thumbnail,
        }


# ---------------------------------------------------------------------------
# Product image — Upload
# ---------------------------------------------------------------------------

def upload_product_image(
    file_path: str | Path,
    item_id: str,
    category: str,
) -> ProductImageURLs:
    """
    Full pipeline: validate → convert → resize → upload all three variants.

    Object key structure::

        products/{category}/{variant}/{item_id}.jpg
    """
    client = MinioClient.get_instance()
    path = Path(file_path)

    # 1. Validate extension before doing any I/O
    validate_image_file(path)

    # 2. Process all variants (converts + resizes → JPEG bytes)
    logger.info(
        "Processing image '%s' → item_id='%s', category='%s'",
        path.name, item_id, category,
    )
    variant_bytes: dict[ImageVariant, bytes] = process_all_variants(path)

    # 3. Upload each variant and collect public URLs
    urls: dict[ImageVariant, str] = {}
    for variant, data in variant_bytes.items():
        key = _product_key(category, variant, item_id)
        _upload_bytes_to_products(client, data, key)
        urls[variant] = client.public_url(BUCKET_PRODUCTS, key)
        logger.info(
            "Uploaded %s variant → '%s/%s' (%d bytes)",
            variant.value, BUCKET_PRODUCTS, key, len(data),
        )

    return ProductImageURLs(
        original=urls[ImageVariant.ORIGINAL],
        large=urls[ImageVariant.LARGE],
        thumbnail=urls[ImageVariant.THUMBNAIL],
        item_id=item_id,
        category=category,
    )


# def upload_product_images(items: list[dict]) -> list[ProductImageURLs]:
#     """
#     Batch upload multiple product images.
#     """
#     return [
#         upload_product_image(
#             file_path=item["file_path"],
#             item_id=item["item_id"],
#             category=item["category"],
#         )
#         for item in items
#     ]


def upload_temp_file(file_path: str | Path, object_name: str | None = None) -> str:
    """
    Upload a raw file (.zip, .csv, etc.) to the private ``uploads-temp`` bucket.

    The file is automatically deleted by MinIO after 24 hours (lifecycle rule
    applied at startup).
    """
    client = MinioClient.get_instance()
    path = Path(file_path)

    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")

    key = object_name or path.name

    try:
        client.raw.fput_object(
            bucket_name=BUCKET_UPLOADS_TEMP,
            object_name=key,
            file_path=str(path),
        )
        logger.info("Uploaded temp file '%s' → '%s/%s'", path.name, BUCKET_UPLOADS_TEMP, key)
    except S3Error as exc:
        raise MinioOperationError(
            "upload_temp_file",
            f"Failed to upload '{path}' to '{BUCKET_UPLOADS_TEMP}'",
            exc,
        ) from exc

    return client.presigned_url(BUCKET_UPLOADS_TEMP, key, TEMP_URL_EXPIRY)


# ---------------------------------------------------------------------------
# Product image — Download
# ---------------------------------------------------------------------------

def download_product_image(
    item_id: str,
    category: str,
    variant: ImageVariant = ImageVariant.ORIGINAL,
) -> bytes:
    """
    Download one variant of a product image from the ``products`` bucket.
    """
    key = f"{category.strip('/')}/{variant.value}/{item_id}.jpg"
    return _download_bytes(BUCKET_PRODUCTS, key)


def download_temp_file(
    url_or_key: str,
    dest_dir: str | Path | None = None,
) -> Path:
    """
    Download a file from the private ``uploads-temp`` bucket and save to disk.

    The caller is responsible for cleaning up the file after processing.
    """
    client = MinioClient.get_instance()
    # If the string contains "/" it may be a full URL or bucket/key — parse it;
    # otherwise treat it as a bare filename already in uploads-temp.
    if "/" in url_or_key:
        _, key = client.object_key_from_url(url_or_key)
    else:
        key = url_or_key

    filename = Path(key).name or "download"
    dest_dir = Path(dest_dir) if dest_dir else Path(tempfile.gettempdir())
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filename

    try:
        client.raw.fget_object(BUCKET_UPLOADS_TEMP, key, str(dest_path))
        logger.info("Downloaded temp file '%s' → '%s'", key, dest_path)
        return dest_path
    except S3Error as exc:
        raise MinioOperationError(
            "download_temp_file",
            f"Failed to download '{BUCKET_UPLOADS_TEMP}/{key}'",
            exc,
        ) from exc


# ---------------------------------------------------------------------------
# Product image — Delete
# ---------------------------------------------------------------------------

def delete_product_image(
    item_id: str,
    category: str,
    variant: ImageVariant | None = None,
) -> None:
    """
    Delete one or all variants of a product image from the ``products`` bucket.
    """
    variants_to_delete = [variant] if variant else list(ImageVariant)
    keys = [
        f"{category.strip('/')}/{v.value}/{item_id}.jpg"
        for v in variants_to_delete
    ]
    _delete_keys(BUCKET_PRODUCTS, keys)


def delete_product_images(items: list[dict]) -> None:
    """
    Delete all variants for multiple products (best-effort batch delete)
    """
    keys = [
        f"{item['category'].strip('/')}/{v.value}/{item['item_id']}.jpg"
        for item in items
        for v in ImageVariant
    ]
    _delete_keys(BUCKET_PRODUCTS, keys)


def delete_temp_file(url_or_key: str) -> None:
    """
    Delete a file from the private ``uploads-temp`` bucket.
    """
    client = MinioClient.get_instance()
    if "/" in url_or_key:
        _, key = client.object_key_from_url(url_or_key)
    else:
        key = url_or_key
    _delete_keys(BUCKET_UPLOADS_TEMP, [key])
