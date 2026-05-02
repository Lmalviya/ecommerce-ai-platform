from pydantic import SecretStr, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class QdrantSettings(BaseSettings):
    """Configuration for the Qdrant Vector Store."""
    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8",
        extra="ignore",
        env_prefix="QDRANT_"
    )

    host: str = Field(..., env="QDRANT_HOST", description="Qdrant server host")
    port: int = Field(..., env="QDRANT_PORT", description="Qdrant server port")
    api_key: Optional[SecretStr] = Field(..., env="QDRANT_API_KEY", description="Qdrant API key")
    
    # Defaults
    default_collection: str = Field(default="products")
    timeout: int = 60

settings = QdrantSettings()
