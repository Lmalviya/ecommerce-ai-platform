from __future__ import annotations

import json
import logging
import urllib.parse
from datetime import timedelta

from minio import Minio
from minio.commonconfig import ENABLED
from minio.error import S3Error
from minio.lifecycleconfig import Expiration, LifecycleConfig, Rule

from ecom_storage.configs.minio_config import (
    BUCKET_PRODUCTS,
    BUCKET_UPLOADS_TEMP,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
    TEMP_URL_EXPIRY,
)
from utils.exceptions import MinioOperationError

try:
    from ecom_shared.logging import get_logger
    logger = get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public-read bucket policy template (S3-compatible)
# ---------------------------------------------------------------------------
def _public_read_policy(bucket: str) -> str:
    """Return a JSON string for a public-read bucket policy."""
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": ["*"]},
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket}/*"],
            }
        ],
    })


# ---------------------------------------------------------------------------
# 24-hour lifecycle rule for uploads-temp
# ---------------------------------------------------------------------------
_TEMP_LIFECYCLE = LifecycleConfig(
    [
        Rule(
            ENABLED,
            rule_id="auto-delete-24h",
            expiration=Expiration(days=1),
        )
    ]
)


class MinioClient:
    """Thread-safe singleton wrapper around ``minio.Minio``."""

    _instance: MinioClient | None = None

    def __init__(self) -> None:
        self._client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        self._provision_buckets()
        logger.info("MinioClient initialised (endpoint=%s)", MINIO_ENDPOINT)

    # ------------------------------------------------------------------
    # Singleton accessor
    # ------------------------------------------------------------------

    @classmethod
    def get_instance(cls) -> MinioClient:
        """Return the shared ``MinioClient`` (creates it on first call)."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ------------------------------------------------------------------
    # Bucket provisioning (runs once at startup)
    # ------------------------------------------------------------------

    def _provision_buckets(self) -> None:
        """Create and configure both buckets if they do not already exist."""
        self._ensure_bucket(BUCKET_UPLOADS_TEMP)
        self._apply_lifecycle(BUCKET_UPLOADS_TEMP, _TEMP_LIFECYCLE)

        self._ensure_bucket(BUCKET_PRODUCTS)
        self._apply_public_read_policy(BUCKET_PRODUCTS)

    def _ensure_bucket(self, bucket: str) -> None:
        """Create *bucket* if it does not already exist."""
        try:
            if not self._client.bucket_exists(bucket):
                self._client.make_bucket(bucket)
                logger.info("Created MinIO bucket: %s", bucket)
        except S3Error as exc:
            raise MinioOperationError(
                "ensure_bucket",
                f"Cannot ensure bucket '{bucket}' exists",
                exc,
            ) from exc

    def _apply_lifecycle(self, bucket: str, config: LifecycleConfig) -> None:
        """Apply a lifecycle configuration to *bucket*."""
        try:
            self._client.set_bucket_lifecycle(bucket, config)
            logger.info(
                "Applied lifecycle rule to bucket '%s' (auto-delete after 24 h)", bucket
            )
        except S3Error as exc:
            raise MinioOperationError(
                "apply_lifecycle",
                f"Failed to set lifecycle on bucket '{bucket}'",
                exc,
            ) from exc

    def _apply_public_read_policy(self, bucket: str) -> None:
        """Set a public-read policy on *bucket*."""
        try:
            self._client.set_bucket_policy(bucket, _public_read_policy(bucket))
            logger.info("Set public-read policy on bucket '%s'", bucket)
        except S3Error as exc:
            raise MinioOperationError(
                "apply_policy",
                f"Failed to set public-read policy on bucket '{bucket}'",
                exc,
            ) from exc

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    def presigned_url(
        self,
        bucket: str,
        object_name: str,
        expiry: timedelta = TEMP_URL_EXPIRY,
    ) -> str:
        """
        Generate a presigned GET URL for a **private** bucket object.

        Use ``public_url()`` instead for objects in the public ``products`` bucket.
        """
        try:
            url = self._client.presigned_get_object(bucket, object_name, expires=expiry)
            logger.debug("Generated presigned URL for '%s/%s'", bucket, object_name)
            return url
        except S3Error as exc:
            raise MinioOperationError(
                "presigned_url",
                f"Cannot generate URL for '{bucket}/{object_name}'",
                exc,
            ) from exc

    def public_url(self, bucket: str, object_name: str) -> str:
        """
        Build a direct (non-expiring) public URL for an object.

        Only use for objects in the public-read ``products`` bucket.
        """
        scheme = "https" if MINIO_SECURE else "http"
        return f"{scheme}://{MINIO_ENDPOINT}/{bucket}/{object_name}"

    # @staticmethod
    # def object_key_from_url(url: str) -> tuple[str, str]:
    #     """
    #     Extract (bucket, object_key) from a plain ``bucket/key`` string or a URL.

    #     Examples
    #     --------
    #     ``"products/electronics/thumbnail/B07.jpg"``
    #         → ``("products", "electronics/thumbnail/B07.jpg")``
    #     ``"http://localhost:9000/products/electronics/thumbnail/B07.jpg"``
    #         → ``("products", "electronics/thumbnail/B07.jpg")``
    #     """
    #     if url.startswith("http://") or url.startswith("https://"):
    #         parsed = urllib.parse.urlparse(url)
    #         parts = parsed.path.lstrip("/").split("/", 1)
    #     else:
    #         parts = url.split("/", 1)

    #     if len(parts) != 2:
    #         raise ValueError(
    #             f"Cannot parse bucket/key from '{url}'. "
    #             "Expected format: 'bucket/path/to/object' or a full URL."
    #         )
    #     return urllib.parse.unquote(parts[0]), urllib.parse.unquote(parts[1])

    # ------------------------------------------------------------------
    # Raw client accessor
    # ------------------------------------------------------------------

    @property
    def raw(self) -> Minio:
        """Direct access to the underlying ``minio.Minio`` client."""
        return self._client
