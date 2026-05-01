from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Connection settings
# ---------------------------------------------------------------------------
QDRANT_HOST: str = os.getenv("QDRANT_HOST")
QDRANT_PORT: int = int(os.getenv("QDRANT_PORT"))
QDRANT_API_KEY: str | None = os.getenv("QDRANT_API_KEY")  # None in local dev

# ---------------------------------------------------------------------------
# Collection settings
# ---------------------------------------------------------------------------
QDRANT_COLLECTION_PRODUCT: str = os.getenv("QDRANT_COLLECTION_PRODUCT")
QDRANT_VECTOR_SIZE: int = int(os.getenv("QDRANT_VECTOR_SIZE"))

# ---------------------------------------------------------------------------
# Fail-fast validation — runs once at import / service startup
# ---------------------------------------------------------------------------
_REQUIRED: dict[str, str] = {
    "QDRANT_HOST": QDRANT_HOST,
    "QDRANT_PORT": QDRANT_PORT,
    "QDRANT_COLLECTION_PRODUCT": QDRANT_COLLECTION_PRODUCT,
    "QDRANT_VECTOR_SIZE": QDRANT_VECTOR_SIZE,
}

_missing = [name for name, value in _REQUIRED.items() if not value]

if _missing:
    raise EnvironmentError(
        f"Missing required Qdrant environment variable(s): {', '.join(_missing)}. "
        "Set them in your .env file or container environment before starting the service."
    )
