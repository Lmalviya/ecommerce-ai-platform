import logging
import structlog
from fastapi import FastAPI, Request
from .api.endpoints import router
from .database import DatabaseClient

# Setup structured logging
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

app = FastAPI(
    title="E-commerce AI Platform API",
    description="The control plane for managing connectors, indexing jobs, and platform health.",
    version="1.0.0"
)

# ---------------------------------------------------------------------------
# DDL for Management Tables
# ---------------------------------------------------------------------------
_CREATE_MANAGEMENT_SCHEMA = """
CREATE TABLE IF NOT EXISTS connector_profiles (
    id          UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT          NOT NULL,
    type        TEXT          NOT NULL,
    config      JSONB         NOT NULL,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingestion_jobs (
    id              UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    profile_id      UUID          REFERENCES connector_profiles(id),
    status          TEXT          NOT NULL DEFAULT 'PENDING',
    total_items     INTEGER       DEFAULT 0,
    processed_items INTEGER       DEFAULT 0,
    failed_items    INTEGER       DEFAULT 0,
    error_message   TEXT,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
"""

@app.on_event("startup")
async def startup_event():
    logger.info("platform_api_starting")
    # Provision management tables
    client = DatabaseClient.get_instance()
    with client.pool.connection() as conn:
        conn.execute(_CREATE_MANAGEMENT_SCHEMA)
        conn.commit()
    logger.info("management_schema_provisioned")

@app.on_event("shutdown")
def shutdown_event():
    DatabaseClient.get_instance().close()
    logger.info("platform_api_shutdown")

# Include Routes
app.include_router(router, prefix="/api/v1")

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "platform-api"}
