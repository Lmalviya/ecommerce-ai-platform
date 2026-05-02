from pydantic import BaseModel, Field
from typing import List, TypeVar, Generic, Dict, Any, Optional
from enum import Enum

class DistanceMetric(str, Enum):
    COSINE = "cosine"
    EUCLIDEAN = "euclidean"
    DOT = "dot"
    MANHATTAN = "manhattan"

class PayloadIndexType(str, Enum):
    KEYWORD = "keyword"
    INTEGER = "integer"
    FLOAT = "float"
    TEXT = "text"

class VectorParams(BaseModel):
    """Configuration for a single named vector."""
    size: int
    distance: DistanceMetric = DistanceMetric.COSINE

T = TypeVar("T", bound=BaseModel)

class VectorPoint(BaseModel, Generic[T]):
    """
    Supports both single vector and named vectors.
    """
    id: str | int
    # For single vector collections
    vector: Optional[List[float]] = None
    # For multi-vector collections
    vectors: Optional[Dict[str, List[float]]] = None
    payload: T

class SearchResult(BaseModel, Generic[T]):
    """Standardized search result."""
    id: str | int
    score: float
    payload: T
    # If using grouping, this will contain the other matches in the group
    group_hits: Optional[List[Dict[str, Any]]] = None