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
    openai_api_key: Optional[SecretStr] = Field(None, env="OPENAI_API_KEY", description="OpenAI API key")
    anthropic_api_key: Optional[SecretStr] = Field(None, env="ANTHROPIC_API_KEY", description="Anthropic API key")
    google_api_key: Optional[SecretStr] = Field(None, env="GOOGLE_API_KEY", description="Google API key")
    
    # Defaults
    default_text_model: str = Field(..., env="GENERATOR_MODEL", description="Default text generation model")
    default_embedding_model: str = Field(..., env="EMBEDDER_MODEL", description="Default embedding model")
    default_api_base: Optional[str] = Field(..., env="API_BASE", description="Default API base")
    
    # Resilience
    max_retries: Optional[int] = Field(3, env="MAX_RETRIES", description="Default max retries")
    timeout_seconds: Optional[float] = Field(60.0, env="TIMEOUT", description="Default timeout")

# Singleton instance to be used across the package
settings = Settings()
