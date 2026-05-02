import time
import litellm
import structlog
from typing import List, Optional, Type, Any
from pydantic import BaseModel
from py_ai_models.config import settings
from py_ai_models.models.requests import GenerationRequest, Message
from py_ai_models.models.responses import GenerationResponse, TokenUsage
from py_ai_models.exceptions import ProviderError

logger = structlog.get_logger()

class AIGenerator:
    """
    Client for generating text and multimodal content.
    Supports standard chat and structured outputs.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or (settings.openai_api_key.get_secret_value() if settings.openai_api_key else None)

    async def generate(self, request: GenerationRequest) -> GenerationResponse:
        """
        Generates a standard chat completion.
        """
        model = request.model or settings.default_text_model
        api_base = request.api_base or settings.default_api_base
        start_time = time.perf_counter()

        # Log the request for observability
        logger.info("ai_generation_started", model=model, api_base=api_base)

        try:
            # LiteLLM's async completion call
            response = await litellm.acompletion(
                model=model,
                messages=[m.model_dump() for m in request.messages],
                temperature=request.temperature,
                max_tokens=request.max_tokens,
                api_key=self.api_key,
                api_base=api_base,
                timeout=settings.timeout_seconds,
                **request.extra_params
            )

            end_time = time.perf_counter()
            latency_ms = (end_time - start_time) * 1000
            
            content = response.choices[0].message.content
            usage = response.usage

            # Standardized response
            gen_response = GenerationResponse(
                content=content,
                model=model,
                usage=TokenUsage(
                    prompt_tokens=usage.prompt_tokens,
                    completion_tokens=usage.completion_tokens,
                    total_tokens=usage.total_tokens
                ),
                latency_ms=latency_ms,
                raw_response=response.model_dump()
            )

            logger.info("ai_generation_completed", model=model, latency_ms=latency_ms, total_tokens=usage.total_tokens)
            return gen_response

        except Exception as e:
            logger.error("ai_generation_failed", model=model, error=str(e))
            raise ProviderError(f"Generation failed: {str(e)}", provider=model)

    async def generate_structured(self, request: GenerationRequest, response_schema: Type[BaseModel]) -> Any:
        """
        Generates a response that is automatically parsed into a Pydantic model.
        Useful for NER, classification, or data extraction.
        """
        # We leverage LiteLLM's 'response_format' feature
        request.extra_params["response_format"] = response_schema
        
        response = await self.generate(request)
        
        # In a real implementation, you might want more robust parsing logic here
        # but LiteLLM handles the heavy lifting of ensuring JSON mode/parsing.
        return response_schema.model_validate_json(response.content)
