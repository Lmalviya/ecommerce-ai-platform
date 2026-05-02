import time
import structlog
from typing import List, Optional, Type, TypeVar, Any, Dict, Union
from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models as rest
from pydantic import BaseModel

from .config import settings
from .exceptions import QdrantConnectionError, CollectionNotFoundError, SearchError
from .models import VectorPoint, SearchResult, DistanceMetric, VectorParams, PayloadIndexType

logger = structlog.get_logger()
T = TypeVar("T", bound=BaseModel)

class AsyncQdrantStore:
    """
    Standardized Async Qdrant Client.
    Handles high-performance vector operations with built-in observability.
    """

    def __init__(
        self, 
        host: Optional[str] = None, 
        port: Optional[int] = None, 
        api_key: Optional[str] = None
    ):
        self.client = AsyncQdrantClient(
            host=host or settings.host,
            port=port or settings.port,
            api_key=api_key or (settings.api_key.get_secret_value() if settings.api_key else None)
        )

    async def health_check(self) -> bool:
        """Checks if the Qdrant server is reachable."""
        try:
            await self.client.get_collections()
            return True
        except Exception:
            return False

    async def collection_exists(self, name: str) -> bool:
        """Checks if a collection exists."""
        collections = await self.client.get_collections()
        return any(c.name == name for c in collections.collections)

    async def create_collection(
        self, 
        name: str, 
        vectors_config: Union[Dict[str, VectorParams], VectorParams]
    ):
        """Creates a collection asynchronously, with an existence check."""
        if await self.collection_exists(name):
            logger.info("collection_already_exists", collection=name)
            return

        try:
            dist_map = {
                DistanceMetric.COSINE: rest.Distance.COSINE,
                DistanceMetric.EUCLIDEAN: rest.Distance.EUCLID,
                DistanceMetric.DOT: rest.Distance.DOT,
                DistanceMetric.MANHATTAN: rest.Distance.MANHATTAN
            }

            if isinstance(vectors_config, VectorParams):
                q_config = rest.VectorParams(
                    size=vectors_config.size,
                    distance=dist_map.get(vectors_config.distance, rest.Distance.COSINE)
                )
            else:
                q_config = {
                    v_name: rest.VectorParams(
                        size=v_params.size,
                        distance=dist_map.get(v_params.distance, rest.Distance.COSINE)
                    ) for v_name, v_params in vectors_config.items()
                }

            await self.client.create_collection(collection_name=name, vectors_config=q_config)
            logger.info("collection_created", collection=name)
        except Exception as e:
            logger.error("collection_creation_failed", collection=name, error=str(e))
            raise

    async def create_payload_index(
        self, 
        collection_name: str, 
        field_name: str, 
        field_type: PayloadIndexType = PayloadIndexType.KEYWORD
    ):
        """Creates a performance index on a payload field."""
        mapping = {
            PayloadIndexType.KEYWORD: rest.PayloadSchemaType.KEYWORD,
            PayloadIndexType.INTEGER: rest.PayloadSchemaType.INTEGER,
            PayloadIndexType.FLOAT: rest.PayloadSchemaType.FLOAT,
            PayloadIndexType.TEXT: rest.PayloadSchemaType.TEXT
        }

        try:
            await self.client.create_payload_index(
                collection_name=collection_name,
                field_name=field_name,
                field_schema=mapping.get(field_type, rest.PayloadSchemaType.KEYWORD)
            )
            logger.info("payload_index_created", collection=collection_name, field=field_name, type=field_type)
        except Exception as e:
            logger.error("payload_index_failed", collection=collection_name, field=field_name, error=str(e))
            raise

    async def upsert_batch(self, collection_name: str, points: List[VectorPoint[T]]):
        """Uploads a batch of points asynchronously."""
        start_time = time.perf_counter()
        try:
            q_points = [
                rest.PointStruct(
                    id=p.id,
                    vector=p.vectors if p.vectors else p.vector,
                    payload=p.payload.model_dump()
                ) for p in points
            ]
            await self.client.upsert(collection_name=collection_name, points=q_points)
            
            latency = (time.perf_counter() - start_time) * 1000
            logger.info("upsert_completed", collection=collection_name, count=len(points), latency_ms=latency)
        except Exception as e:
            logger.error("upsert_failed", collection=collection_name, error=str(e))
            raise

    async def delete_points(self, collection_name: str, ids: List[Union[str, int]]):
        """Deletes specific points from a collection."""
        try:
            await self.client.delete(
                collection_name=collection_name,
                points_selector=rest.PointIdsList(points=ids)
            )
            logger.info("points_deleted", collection=collection_name, count=len(ids))
        except Exception as e:
            logger.error("delete_failed", collection=collection_name, error=str(e))
            raise

    async def search(
        self, 
        collection_name: str, 
        query_vector: List[float], 
        payload_model: Type[T],
        vector_name: Optional[str] = None,
        group_by: Optional[str] = None,
        limit: int = 10,
        group_size: int = 3,
        filter_dict: Optional[Dict[str, Any]] = None
    ) -> List[SearchResult[T]]:
        """Universal search method supporting both standard and grouped search."""
        start_time = time.perf_counter()
        
        try:
            # Build Filter
            q_filter = self._build_filter(filter_dict)
            v_query = (vector_name, query_vector) if vector_name else query_vector

            if group_by:
                results = await self.client.search_groups(
                    collection_name=collection_name,
                    query_vector=v_query,
                    group_by=group_by,
                    limit=limit,
                    group_size=group_size,
                    query_filter=q_filter,
                    with_payload=True
                )
                formatted_results = [
                    SearchResult(
                        id=str(group.hits[0].id),
                        score=group.hits[0].score,
                        payload=payload_model(**group.hits[0].payload),
                        group_hits=[h.model_dump() for h in group.hits]
                    ) for group in results.groups
                ]
            else:
                results = await self.client.search(
                    collection_name=collection_name,
                    query_vector=v_query,
                    limit=limit,
                    query_filter=q_filter,
                    with_payload=True
                )
                formatted_results = [
                    SearchResult(id=str(r.id), score=r.score, payload=payload_model(**r.payload))
                    for r in results
                ]

            latency = (time.perf_counter() - start_time) * 1000
            logger.info("search_completed", collection=collection_name, latency_ms=latency, results_count=len(formatted_results))
            return formatted_results

        except Exception as e:
            logger.error("search_failed", collection=collection_name, error=str(e))
            raise SearchError(f"Search failed: {str(e)}")

    def _build_filter(self, filter_dict: Optional[Dict[str, Any]]) -> Optional[rest.Filter]:
        """Helper to build Qdrant filters from a dictionary."""
        if not filter_dict:
            return None
        conditions = []
        for k, v in filter_dict.items():
            if isinstance(v, dict):
                conditions.append(rest.FieldCondition(
                    key=k, range=rest.Range(gt=v.get('gt'), gte=v.get('gte'), lt=v.get('lt'), lte=v.get('lte'))
                ))
            elif isinstance(v, list):
                conditions.append(rest.FieldCondition(key=k, match=rest.MatchAny(any=v)))
            else:
                conditions.append(rest.FieldCondition(key=k, match=rest.MatchValue(value=v)))
        return rest.Filter(must=conditions)
