import logging
from typing import List, Optional, Type, TypeVar, Any, Dict
from qdrant_client import QdrantClient
from qdrant_client.http import models as rest
from pydantic import BaseModel
from .models import VectorPoint, SearchResult, DistanceMetric, VectorParams, PayloadIndexType

T = TypeVar("T", bound=BaseModel)

class QdrantStore:
    """
    Advanced Qdrant Client.
    Supports Named Vectors, Grouping, and Polymorphic Search.
    """

    def __init__(self, host: str, port: int = 6333, api_key: Optional[str] = None):
        self.client = QdrantClient(host=host, port=port, api_key=api_key)

    def create_collection(self, name: str, vectors_config: Dict[str, VectorParams] | VectorParams):
        """Creates a collection with single or named vectors."""
        collections = self.client.get_collections().collections
        if any(c.name == name for c in collections):
            logging.info(f"Collection '{name}' already exists.")
            return

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

        self.client.create_collection(collection_name=name, vectors_config=q_config)
        logging.info(f"Successfully created collection '{name}'")

    def create_payload_index(
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
            PayloadIndexType.TEXT: rest.PayloadSchemaType.TEXT        }

        self.client.create_payload_index(
            collection_name=collection_name,
            field_name=field_name,
            field_schema=mapping.get(field_type, rest.PayloadSchemaType.KEYWORD)
        )
        logging.info(f"Indexed field '{field_name}' as {field_type}")

    def upsert_batch(self, collection_name: str, points: List[VectorPoint[T]]):
        """Uploads points, detecting single or named vectors."""
        q_points = [
            rest.PointStruct(
                id=p.id,
                vector=p.vectors if p.vectors else p.vector,
                payload=p.payload.model_dump()
            ) for p in points
        ]
        self.client.upsert(collection_name=collection_name, points=q_points)

    def _build_filter(self, filter_dict: Optional[Dict[str, Any]]) -> Optional[rest.Filter]:
        """Helper to build complex Qdrant filters from a dictionary."""
        if not filter_dict:
            return None

        conditions = []
        for k, v in filter_dict.items():
            if isinstance(v, dict):
                conditions.append(rest.FieldCondition(
                    key=k, 
                    range=rest.Range(gt=v.get('gt'), gte=v.get('gte'), lt=v.get('lt'), lte=v.get('lte'))
                ))
            elif isinstance(v, list):
                conditions.append(rest.FieldCondition(key=k, match=rest.MatchAny(any=v)))
            else:
                conditions.append(rest.FieldCondition(key=k, match=rest.MatchValue(value=v)))
        
        return rest.Filter(must=conditions)

    def _format_query_vector(self, vector: List[float], name: Optional[str]) -> Any:
        """Helper to format vector for named or unnamed search."""
        return (name, vector) if name else vector

    def search(
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
        """Universal search method (Refactored for cleanliness)."""
        
        q_filter = self._build_filter(filter_dict)
        v_query = self._format_query_vector(query_vector, vector_name)

        if group_by:
            return self._search_grouped(
                collection_name, v_query, payload_model, group_by, limit, group_size, q_filter
            )
        
        return self._search_standard(
            collection_name, v_query, payload_model, limit, q_filter
        )

    def _search_standard(self, collection, vector, model, limit, q_filter) -> List[SearchResult[T]]:
        """Executes a standard vector search."""
        results = self.client.search(
            collection_name=collection,
            query_vector=vector,
            limit=limit,
            query_filter=q_filter,
            with_payload=True
        )
        return [
            SearchResult(id=str(r.id), score=r.score, payload=model(**r.payload))
            for r in results
        ]

    def _search_grouped(self, collection, vector, model, group_by, limit, size, q_filter) -> List[SearchResult[T]]:
        """Executes a grouped vector search."""
        results = self.client.search_groups(
            collection_name=collection,
            query_vector=vector,
            group_by=group_by,
            limit=limit,
            group_size=size,
            query_filter=q_filter,
            with_payload=True
        )
        return [
            SearchResult(
                id=str(group.hits[0].id),
                score=group.hits[0].score,
                payload=model(**group.hits[0].payload),
                group_hits=[h.model_dump() for h in group.hits]
            ) for group in results.groups
        ]
