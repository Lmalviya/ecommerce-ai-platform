from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class TokenUsage(BaseModel):
    """Standardized tracking for token usage and cost."""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

class GenerationResponse(BaseModel):
    """Standardized response for text/multimodal generation."""
    content: str
    model: str
    usage: TokenUsage
    latency_ms: float
    raw_response: Optional[Dict[str, Any]] = None

class EmbeddingResponse(BaseModel):
    """Standardized response for embeddings."""
    embeddings: List[List[float]]
    model: str
    usage: TokenUsage
    latency_ms: float

class ImageResponse(BaseModel):
    """Standardized response for image generation."""
    urls: List[str]
    b64_json: Optional[List[str]] = None
    model: str
    latency_ms: float

class AudioResponse(BaseModel):
    """Standardized response for audio generation or transcription."""
    content: Union[str, bytes] = Field(..., description="Transcript text OR raw audio bytes")
    model: str
    latency_ms: float
