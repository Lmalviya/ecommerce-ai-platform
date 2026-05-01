from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from qdrant_client.models import (
    FieldCondition,
    Filter,
    MatchValue,
    PointIdsList,
    PointStruct,
)

from ecom_storage.clients.qdrant_client import QdrantClientSingleton
from ecom_storage.configs.qdrant_config import QDRANT_COLLECTION_PRODUCT

try:
    from ecom_shared.logging import get_logger
    logger = get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class VectorPoint:
    """
    A single vector point to upsert into Qdrant.

    Attributes
    ----------
    item_id:
        The product's item_id (used as the Qdrant point ID).
    embedding:
        The vector produced by the embedding model.
    payload:
        Metadata stored alongside the vector for filtering.
        Recommended fields: brand, product_type, category, name.
    """
    item_id: str
    embedding: list[float]
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class SearchResult:
    """A single result from a similarity search."""
    item_id: str
    score: float
    payload: dict[str, Any]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_filter(filters: dict | None) -> Filter | None:
    """
    Convert a simple key=value dict into a Qdrant Filter.

    Example
    -------
    {"brand": "Nike", "product_type": "shoes"}
    → Filter(must=[FieldCondition(key="brand", match=MatchValue(value="Nike")),
                   FieldCondition(key="product_type", match=MatchValue(value="shoes"))])
    """
    if not filters:
        return None
    conditions = [
        FieldCondition(key=k, match=MatchValue(value=v))
        for k, v in filters.items()
    ]
    return Filter(must=conditions)


# ---------------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------------

def upsert_vector(
    item_id: str,
    embedding: list[float],
    payload: dict[str, Any] | None = None,
) -> None:
    """
    Insert or update a single vector point in the products collection.

    Parameters
    ----------
    item_id:
        The product's unique identifier (used as Qdrant point ID).
    embedding:
        The vector to store (must match QDRANT_VECTOR_SIZE dimensions).
    payload:
        Optional metadata for filtering (e.g. brand, product_type).
    """
    client = QdrantClientSingleton.get_instance()
    point = PointStruct(
        id=item_id,
        vector=embedding,
        payload=payload or {},
    )
    client.raw.upsert(
        collection_name=QDRANT_COLLECTION_PRODUCT,
        points=[point],
    )
    logger.info("Upserted vector for item_id='%s' into '%s'", item_id, QDRANT_COLLECTION_PRODUCT)


def upsert_vectors(points: list[VectorPoint]) -> None:
    """
    Batch insert or update multiple vector points.
    More efficient than calling upsert_vector() in a loop.
    """
    client = QdrantClientSingleton.get_instance()
    qdrant_points = [
        PointStruct(
            id=p.item_id,
            vector=p.embedding,
            payload=p.payload,
        )
        for p in points
    ]
    client.raw.upsert(
        collection_name=QDRANT_COLLECTION_PRODUCT,
        points=qdrant_points,
    )
    logger.info("Batch upserted %d vectors into '%s'", len(points), QDRANT_COLLECTION_PRODUCT)


def search_vectors(
    query_vector: list[float],
    top_k: int = 20,
    filters: dict | None = None,
) -> list[SearchResult]:
    """
    Run a cosine similarity search against the products collection.

    Parameters
    ----------
    query_vector:
        The embedding of the search query.
    top_k:
        Maximum number of results to return.
    filters:
        Optional key=value pairs to filter results by payload fields.
        Example: {"brand": "Nike", "product_type": "shoes"}

    Returns
    -------
    List of SearchResult objects sorted by descending score.
    """
    client = QdrantClientSingleton.get_instance()
    results = client.raw.search(
        collection_name=QDRANT_COLLECTION_PRODUCT,
        query_vector=query_vector,
        limit=top_k,
        query_filter=_build_filter(filters),
        with_payload=True,
    )
    return [
        SearchResult(
            item_id=str(r.id),
            score=r.score,
            payload=r.payload or {},
        )
        for r in results
    ]


def get_vector(item_id: str) -> VectorPoint | None:
    """
    Retrieve a single stored vector point by item_id.
    Returns None if the point does not exist.
    """
    client = QdrantClientSingleton.get_instance()
    results = client.raw.retrieve(
        collection_name=QDRANT_COLLECTION_PRODUCT,
        ids=[item_id],
        with_vectors=True,
        with_payload=True,
    )
    if not results:
        return None
    point = results[0]
    return VectorPoint(
        item_id=str(point.id),
        embedding=point.vector,
        payload=point.payload or {},
    )


def delete_vector(item_id: str) -> None:
    """Delete a single vector point by item_id."""
    client = QdrantClientSingleton.get_instance()
    client.raw.delete(
        collection_name=QDRANT_COLLECTION_PRODUCT,
        points_selector=PointIdsList(points=[item_id]),
    )
    logger.info("Deleted vector for item_id='%s' from '%s'", item_id, QDRANT_COLLECTION_PRODUCT)


def delete_vectors(item_ids: list[str]) -> None:
    """Batch delete multiple vector points by item_id."""
    client = QdrantClientSingleton.get_instance()
    client.raw.delete(
        collection_name=QDRANT_COLLECTION_PRODUCT,
        points_selector=PointIdsList(points=item_ids),
    )
    logger.info("Batch deleted %d vectors from '%s'", len(item_ids), QDRANT_COLLECTION)