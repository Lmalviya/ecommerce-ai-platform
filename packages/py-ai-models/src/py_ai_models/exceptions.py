class AIModelError(Exception):
    """Base exception for all AI model related errors."""
    pass

class ConfigurationError(AIModelError):
    """Raised when the package is not configured correctly (e.g., missing API keys)."""
    pass

class ProviderError(AIModelError):
    """Raised when an external AI provider returns an error (e.g., Rate limits, API down)."""
    def __init__(self, message: str, provider: str, status_code: int = None):
        super().__init__(message)
        self.provider = provider
        self.status_code = status_code

class ModelTimeoutError(AIModelError):
    """Raised when a model request exceeds the configured timeout."""
    pass
