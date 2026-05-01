from __future__ import annotations

import logging

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ecom_storage.configs.postgres_config import (
    DATABASE_URL,
    DB_MAX_CONNECTIONS,
    DB_MIN_CONNECTIONS,
)

try:
    from ecom_shared.logging import get_logger
    logger = get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DDL — table and index creation
# ---------------------------------------------------------------------------
_CREATE_PRODUCTS_TABLE = """
CREATE TABLE IF NOT EXISTS products (
    item_id             TEXT        PRIMARY KEY,
    item_name           TEXT        NOT NULL,
    brand               TEXT,
    model_number        TEXT,
    product_type        TEXT,
    country             TEXT,

    colors              TEXT[]      NOT NULL DEFAULT '{}',
    styles              TEXT[]      NOT NULL DEFAULT '{}',
    keywords            TEXT[]      NOT NULL DEFAULT '{}',
    bullet_point        TEXT[]      NOT NULL DEFAULT '{}',

    original_image_id   TEXT,
    thumbnail_image_id  TEXT,
    large_image_id      TEXT,
    other_image_id      TEXT[]      NOT NULL DEFAULT '{}',

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_CREATE_BRAND_INDEX = """
CREATE INDEX IF NOT EXISTS idx_products_brand ON products (brand);
"""

_CREATE_PRODUCT_TYPE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_products_product_type ON products (product_type);
"""

# Automatically update updated_at on every row update
_CREATE_UPDATED_AT_TRIGGER = """
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_products_updated_at'
    ) THEN
        CREATE TRIGGER trg_products_updated_at
        BEFORE UPDATE ON products
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    END IF;
END;
$$;
"""


class PostgresClient:
    """Thread-safe singleton wrapping a psycopg connection pool."""

    _instance: PostgresClient | None = None

    def __init__(self) -> None:
        self._pool = ConnectionPool(
            conninfo=DATABASE_URL,
            min_size=DB_MIN_CONNECTIONS,
            max_size=DB_MAX_CONNECTIONS,
            kwargs={"row_factory": dict_row},
        )
        self._provision_schema()
        logger.info("PostgresClient initialised (pool min=%d, max=%d)", DB_MIN_CONNECTIONS, DB_MAX_CONNECTIONS)

    # ------------------------------------------------------------------
    # Singleton accessor
    # ------------------------------------------------------------------

    @classmethod
    def get_instance(cls) -> PostgresClient:
        """Return the shared PostgresClient (creates it on first call)."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ------------------------------------------------------------------
    # Schema provisioning (runs once at startup)
    # ------------------------------------------------------------------

    def _provision_schema(self) -> None:
        """Create the products table and indexes if they do not already exist."""
        with self._pool.connection() as conn:
            conn.execute(_CREATE_PRODUCTS_TABLE)
            conn.execute(_CREATE_BRAND_INDEX)
            conn.execute(_CREATE_PRODUCT_TYPE_INDEX)
            conn.execute(_CREATE_UPDATED_AT_TRIGGER)
            conn.commit()
        logger.info("PostgreSQL schema provisioned (products table ready)")

    # ------------------------------------------------------------------
    # Pool accessor
    # ------------------------------------------------------------------

    @property
    def pool(self) -> ConnectionPool:
        """Direct access to the underlying connection pool."""
        return self._pool

    def close(self) -> None:
        """Cleanly close all pool connections (call on app shutdown)."""
        self._pool.close()
        logger.info("PostgresClient pool closed")
