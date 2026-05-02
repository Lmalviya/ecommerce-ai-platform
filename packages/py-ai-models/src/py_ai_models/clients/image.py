import time
import litellm
from typing import Optional
from py_ai_models.config import settings
from py_ai_models.models.requests import ImageGenerationRequest
from py_ai_models.models.responses import ImageResponse
from py_ai_models.exceptions import ProviderError

class AIImageClient:
    """
    Client for generating and editing images.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or (settings.openai_api_key.get_secret_value() if settings.openai_api_key else None)

    async def generate(self, request: ImageGenerationRequest) -> ImageResponse:
        """
        Generates images from a text prompt.
        """
        model = request.model or "dall-e-3"
        api_base = request.api_base or settings.default_api_base
        start_time = time.perf_counter()

        try:
            response = await litellm.aimage_generation(
                model=model,
                prompt=request.prompt,
                n=request.n,
                size=request.size,
                quality=request.quality,
                style=request.style,
                api_key=self.api_key,
                api_base=api_base
            )
            
            end_time = time.perf_counter()
            latency_ms = (end_time - start_time) * 1000

            urls = [item["url"] for item in response.data if "url" in item]
            b64 = [item["b64_json"] for item in response.data if "b64_json" in item]

            return ImageResponse(
                urls=urls,
                b64_json=b64 if b64 else None,
                model=model,
                latency_ms=latency_ms
            )

        except Exception as e:
            raise ProviderError(f"Image generation failed: {str(e)}", provider=model)
