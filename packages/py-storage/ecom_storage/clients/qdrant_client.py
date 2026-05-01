from __future__ import annotations

import logging

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

from ecom_storage.configs.qdrant_config import (
    QDRANT_API_KEY,
    QDRANT_COLLECTION_PRODUCT,
    QDRANT_HOST,
    QDRANT_PORT,
    QDRANT_VECTOR_SIZE,
)

try:
    from ecom_shared.logging import get_logger
    logger = get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)


class QdrantClientSingleton:
    """Thread-safe singleton wrapper around ``qdrant_client.QdrantClient``."""

    _instance: QdrantClientSingleton | None = None

    def __init__(self) -> None:
        self._client = QdrantClient(
            host=QDRANT_HOST,
            port=QDRANT_PORT,
            api_key=QDRANT_API_KEY,  # None is fine for local dev (no auth)
        )
        self._provision_collection()
        logger.info(
            "QdrantClient initialised (host=%s, port=%d, collection=%s, vector_size=%d)",
            QDRANT_HOST, QDRANT_PORT, QDRANT_COLLECTION, QDRANT_VECTOR_SIZE,
        )

    # ------------------------------------------------------------------
    # Singleton accessor
    # ------------------------------------------------------------------

    @classmethod
    def get_instance(cls) -> QdrantClientSingleton:
        """Return the shared QdrantClientSingleton (creates it on first call)."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ------------------------------------------------------------------
    # Collection provisioning (runs once at startup)
    # ------------------------------------------------------------------

    def _provision_collection(self) -> None:
        """Create the products collection if it does not already exist."""
        if not self._client.collection_exists(QDRANT_COLLECTION_PRODUCT):
            self._client.create_collection(
                collection_name=QDRANT_COLLECTION_PRODUCT,
                vectors_config=VectorParams(
                    size=QDRANT_VECTOR_SIZE,
                    distance=Distance.COSINE,
                ),
            )
            logger.info("Created Qdrant collection: '%s'", QDRANT_COLLECTION_PRODUCT)
        else:
            logger.info("Qdrant collection '%s' already exists", QDRANT_COLLECTION_PRODUCT)

    # ------------------------------------------------------------------
    # Raw client accessor
    # ------------------------------------------------------------------

    @property
    def raw(self) -> QdrantClient:
        """Direct access to the underlying QdrantClient."""
        return self._client