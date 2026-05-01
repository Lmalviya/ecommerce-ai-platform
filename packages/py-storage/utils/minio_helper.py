"""
utils/minio_helper.py — Internal shared helpers for MinIO operations.

These are low-level primitives shared by image_repo.py.
They are NOT part of the public API — import from ecom_storage, not here.
"""

from __future__ import annotations

import io
import logging

from minio.error import S3Error

from ecom_storage.clients.minio_client import MinioClient
from ecom_storage.configs.minio_config import (
    BUCKET_PRODUCTS,
    OUTPUT_IMAGE_CONTENT_TYPE,
    OUTPUT_IMAGE_EXTENSION,
    ImageVariant,
)
from utils.exceptions import MinioOperationError

try:
    from ecom_shared.logging import get_logger
    logger = get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)


def _delete_keys(bucket: str, keys: list[str]) -> None:
    """
    Delete *keys* from *bucket*, attempting all even if some fail.
    """
    client = MinioClient.get_instance()
    failures: list[str] = []

    for key in keys:
        try:
            client.raw.remove_object(bucket, key)
            logger.info("Deleted '%s/%s'", bucket, key)
        except S3Error as exc:
            logger.error("Failed to delete '%s/%s': %s", bucket, key, exc)
            failures.append(f"{bucket}/{key}")

    if failures:
        raise MinioOperationError(
            "delete",
            f"Failed to delete {len(failures)} object(s): {failures}",
        )


def _download_bytes(bucket: str, key: str) -> bytes:
    """Fetch object *key* from *bucket* and return raw bytes."""
    client = MinioClient.get_instance()
    try:
        response = client.raw.get_object(bucket, key)
        data = response.read()
        response.close()
        response.release_conn()
        logger.debug("Downloaded '%s/%s' (%d bytes)", bucket, key, len(data))
        return data
    except S3Error as exc:
        raise MinioOperationError(
            "download",
            f"Failed to download '{bucket}/{key}'",
            exc,
        ) from exc


def _product_key(category: str, variant: ImageVariant, item_id: str) -> str:
    """
    Build the object key for a product image.

    Format: ``{category}/{variant}/{item_id}.jpg``
    Example: ``electronics/thumbnail/B07P8ML82R.jpg``
    """
    return f"{category.strip('/')}/{variant.value}/{item_id}{OUTPUT_IMAGE_EXTENSION}"


def _upload_bytes_to_products(
    client: MinioClient,
    data: bytes,
    object_name: str,
) -> None:
    """Upload raw *data* bytes to the products bucket."""
    try:
        client.raw.put_object(
            bucket_name=BUCKET_PRODUCTS,
            object_name=object_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type=OUTPUT_IMAGE_CONTENT_TYPE,
        )
    except S3Error as exc:
        raise MinioOperationError(
            "upload_product_image",
            f"Failed to upload '{BUCKET_PRODUCTS}/{object_name}'",
            exc,
        ) from exc
