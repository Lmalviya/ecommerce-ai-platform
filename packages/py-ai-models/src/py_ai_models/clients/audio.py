import time
import litellm
from typing import Optional
from py_ai_models.config import settings
from py_ai_models.models.requests import AudioRequest
from py_ai_models.models.responses import AudioResponse
from py_ai_models.exceptions import ProviderError

class AIAudioClient:
    """
    Client for Text-to-Speech and Speech-to-Text.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or (settings.openai_api_key.get_secret_value() if settings.openai_api_key else None)

    async def text_to_speech(self, request: AudioRequest) -> AudioResponse:
        """
        Converts text into raw audio bytes.
        """
        model = request.model or "tts-1"
        start_time = time.perf_counter()

        try:
            # Note: aspeech/atranscription are LiteLLM wrappers around provider audio APIs
            response = await litellm.aspeech(
                model=model,
                input=request.input,
                voice=request.voice,
                api_key=self.api_key,
                api_base=request.api_base or settings.default_api_base
            )
            
            end_time = time.perf_counter()
            latency_ms = (end_time - start_time) * 1000

            return AudioResponse(
                content=response.content,  # Raw bytes
                model=model,
                latency_ms=latency_ms
            )
        except Exception as e:
            raise ProviderError(f"TTS failed: {str(e)}", provider=model)

    async def transcribe(self, request: AudioRequest) -> AudioResponse:
        """
        Converts audio (file path or bytes) into text.
        """
        model = request.model or "whisper-1"
        start_time = time.perf_counter()

        try:
            response = await litellm.atranscription(
                model=model,
                file=request.input,
                api_key=self.api_key,
                api_base=request.api_base or settings.default_api_base
            )
            
            end_time = time.perf_counter()
            latency_ms = (end_time - start_time) * 1000

            return AudioResponse(
                content=response.text,
                model=model,
                latency_ms=latency_ms
            )
        except Exception as e:
            raise ProviderError(f"Transcription failed: {str(e)}", provider=model)
