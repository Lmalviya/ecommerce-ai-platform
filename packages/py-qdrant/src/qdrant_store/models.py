from pydantic import BaseModel, Field
from typing import List, TypeVar, Generic, Dict, Any, Optional, Union
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

class SparseVectorParams(BaseModel):
    """Configuration for a sparse vector."""
    modifier: Optional[str] = None

class VectorParams(BaseModel):
    """Configuration for a vector (size and distance metric)."""
    size: int
    distance: DistanceMetric = DistanceMetric.COSINE
    use_scalar_quantization: bool = False

T = TypeVar("T", bound=BaseModel)

class VectorPoint(BaseModel, Generic[T]):
    """Standardized point for upserting into Qdrant."""
    id: Union[str, int]
    vector: Optional[List[float]] = None
    vectors: Optional[Dict[str, Any]] = None
    payload: T

class SearchResult(BaseModel, Generic[T]):
    """Standardized search result."""
    id: Union[str, int]
    score: float
    payload: T
    group_hits: Optional[List[Dict[str, Any]]] = None
