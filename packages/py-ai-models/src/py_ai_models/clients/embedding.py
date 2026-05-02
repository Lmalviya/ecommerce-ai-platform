import time
import litellm
from typing import List, Optional, Union
from py_ai_models.config import settings
from py_ai_models.models.requests import EmbeddingRequest
from py_ai_models.models.responses import EmbeddingResponse, TokenUsage
from py_ai_models.exceptions import ProviderError

class AIEmbedder:
    """
    Unified client for generating Text and Image embeddings.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or (settings.openai_api_key.get_secret_value() if settings.openai_api_key else None)

    async def embed(self, request: EmbeddingRequest) -> EmbeddingResponse:
        """
        Generates embeddings for the provided input (text or image).
        """
        model = request.model or settings.default_embedding_model
        api_base = request.api_base or settings.default_api_base
        start_time = time.perf_counter()

        try:
            # LiteLLM handles both text and image embedding inputs
            response = await litellm.aembedding(
                model=model,
                input=request.input,
                api_key=self.api_key,
                api_base=api_base,
                timeout=settings.timeout_seconds
            )
            
            end_time = time.perf_counter()
            latency_ms = (end_time - start_time) * 1000

            # Normalizing usage data (some providers don't return usage for images)
            usage_data = getattr(response, "usage", {"prompt_tokens": 0, "total_tokens": 0})

            return EmbeddingResponse(
                embeddings=[item["embedding"] for item in response["data"]],
                model=model,
                usage=TokenUsage(
                    prompt_tokens=usage_data.get("prompt_tokens", 0),
                    total_tokens=usage_data.get("total_tokens", 0)
                ),
                latency_ms=latency_ms
            )

        except Exception as e:
            raise ProviderError(f"Embedding failed: {str(e)}", provider=model)
