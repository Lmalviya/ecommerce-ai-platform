from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Connection settings
# ---------------------------------------------------------------------------
DATABASE_URL: str = os.getenv("DATABASE_URL")

# Connection pool settings
DB_MIN_CONNECTIONS: int = int(os.getenv("DB_MIN_CONNECTIONS", "1"))
DB_MAX_CONNECTIONS: int = int(os.getenv("DB_MAX_CONNECTIONS", "10"))

# ---------------------------------------------------------------------------
# Fail-fast validation — runs once at import / service startup
# ---------------------------------------------------------------------------
_REQUIRED: dict[str, str] = {
    "DATABASE_URL": DATABASE_URL,
}

_missing = [name for name, value in _REQUIRED.items() if not value]

if _missing:
    raise EnvironmentError(
        f"Missing required PostgreSQL environment variable(s): {', '.join(_missing)}. "
        "Set them in your .env file or container environment before starting the service."
    )
