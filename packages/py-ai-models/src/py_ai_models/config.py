from pydantic import SecretStr, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    """
    Core settings for the AI models package.
    Automatically loads from environment variables or .env files.
    """
    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # API Keys (Using SecretStr to avoid accidental logging)
    openai_api_key: Optional[SecretStr] = None
    anthropic_api_key: Optional[SecretStr] = None
    google_api_key: Optional[SecretStr] = None
    
    # Defaults
    default_text_model: str = Field(default="gpt-4o-mini")
    default_embedding_model: str = Field(default="text-embedding-3-small")
    default_api_base: Optional[str] = Field(default="https://api.openai.com/v1")
    
    # Resilience
    max_retries: int = 3
    timeout_seconds: float = 60.0

# Singleton instance to be used across the package
settings = Settings()
