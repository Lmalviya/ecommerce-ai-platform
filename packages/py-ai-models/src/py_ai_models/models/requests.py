from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union, Literal

class ContentBlock(BaseModel):
    """Represents a piece of multimodal content (text or image)."""
    type: Literal["text", "image_url"]
    text: Optional[str] = None
    image_url: Optional[Dict[str, str]] = None  # e.g., {"url": "..."} or {"url": "data:image/jpeg;base64,..."}

class Message(BaseModel):
    """Represents a single message in a conversation, supporting text and images."""
    role: str = Field(..., description="The role (system, user, assistant)")
    content: Union[str, List[ContentBlock]] = Field(..., description="The content of the message")

class GenerationRequest(BaseModel):
    """Standardized request for text or multimodal generation."""
    model: Optional[str] = None
    api_base: Optional[str] = None
    messages: List[Message]
    temperature: float = 0.7
    max_tokens: Optional[int] = None
    extra_params: Dict[str, Any] = Field(default_factory=dict)

class EmbeddingRequest(BaseModel):
    """Standardized request for generating embeddings from text or images."""
    model: Optional[str] = None
    api_base: Optional[str] = None
    input: Union[str, List[str], bytes] = Field(..., description="Text strings or raw image bytes to embed")

class ImageGenerationRequest(BaseModel):
    """Request for generating or editing images."""
    prompt: str
    model: Optional[str] = Field(default="dall-e-3")
    n: int = 1
    size: str = "1024x1024"
    quality: str = "standard"
    style: str = "vivid"
    api_base: Optional[str] = None

class AudioRequest(BaseModel):
    """Request for Text-to-Speech or Speech-to-Text."""
    input: str = Field(..., description="Text to speak OR path/bytes to transcribe")
    model: Optional[str] = None
    voice: Optional[str] = "alloy"  # For TTS
    response_format: str = "mp3"
    api_base: Optional[str] = None
