from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ecom_storage.repositories.vector_repo import (
    VectorPoint,
    delete_vector,
    delete_vectors,
    get_vector,
    search_vectors,
    upsert_vector,
    upsert_vectors,
)

vector_router = APIRouter(prefix="/storage/vector", tags=["vector-storage"])


# ---------------------------------------------------------------------------
# Request / Response Models
# ---------------------------------------------------------------------------

class UpsertVectorRequest(BaseModel):
    item_id: str
    embedding: list[float]
    payload: dict[str, Any] = {}


class BatchUpsertRequest(BaseModel):
    points: list[UpsertVectorRequest]


class SearchRequest(BaseModel):
    query_vector: list[float]
    top_k: int = 20
    filters: Optional[dict[str, str]] = None


class BatchDeleteRequest(BaseModel):
    item_ids: list[str]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@vector_router.post("/upsert", response_model=dict, status_code=201)
def api_upsert_vector(body: UpsertVectorRequest):
    """
    Insert or update a single vector point in the products collection.
    The embedding must match the configured vector size (default: 1536).
    The payload should include filterable fields like brand, product_type.
    """
    try:
        upsert_vector(
            item_id=body.item_id,
            embedding=body.embedding,
            payload=body.payload,
        )
        return {"success": True, "item_id": body.item_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@vector_router.post("/upsert/batch", response_model=dict, status_code=201)
def api_upsert_vectors_batch(body: BatchUpsertRequest):
    """
    Batch insert or update multiple vector points.
    More efficient than calling /upsert in a loop.
    """
    try:
        points = [
            VectorPoint(
                item_id=p.item_id,
                embedding=p.embedding,
                payload=p.payload,
            )
            for p in body.points
        ]
        upsert_vectors(points)
        return {"success": True, "upserted_count": len(points)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@vector_router.post("/search", response_model=dict)
def api_search_vectors(body: SearchRequest):
    """
    Run a cosine similarity search.
    Optionally filter results by payload fields.

    Example filters: {"brand": "Nike", "product_type": "shoes"}
    """
    try:
        results = search_vectors(
            query_vector=body.query_vector,
            top_k=body.top_k,
            filters=body.filters,
        )
        return {
            "count": len(results),
            "results": [
                {
                    "item_id": r.item_id,
                    "score": r.score,
                    "payload": r.payload,
                }
                for r in results
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@vector_router.get("/{item_id}", response_model=dict)
def api_get_vector(item_id: str):
    """Retrieve a stored vector point and its payload by item_id."""
    try:
        point = get_vector(item_id)
        if point is None:
            raise HTTPException(status_code=404, detail=f"Vector for item_id='{item_id}' not found")
        return {
            "item_id": point.item_id,
            "embedding": point.embedding,
            "payload": point.payload,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@vector_router.delete("/{item_id}", response_model=dict)
def api_delete_vector(item_id: str):
    """Delete a single vector point by item_id."""
    try:
        delete_vector(item_id)
        return {"success": True, "deleted_item_id": item_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@vector_router.delete("/batch/delete", response_model=dict)
def api_delete_vectors_batch(body: BatchDeleteRequest):
    """Batch delete multiple vector points by item_id."""
    try:
        delete_vectors(body.item_ids)
        return {"success": True, "deleted_count": len(body.item_ids)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
