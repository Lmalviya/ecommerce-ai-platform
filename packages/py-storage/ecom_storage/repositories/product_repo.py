from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from ecom_storage.clients.postgres_client import PostgresClient

try:
    from ecom_shared.logging import get_logger
    logger = get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data model (mirrors Prisma Product schema)
# ---------------------------------------------------------------------------

@dataclass
class Product:
    """
    Python representation of the products table row.

    Column mapping (Prisma → Postgres → Python)
    --------------------------------------------
    id               → item_id              → item_id
    name             → item_name            → name
    brand            → brand               → brand
    modelNumber      → model_number         → model_number
    productType      → product_type         → product_type
    country          → country              → country
    colors           → colors              → colors
    styles           → styles              → styles
    keywords         → keywords            → keywords
    bulletPoints     → bullet_point         → bullet_points
    originalImageId  → original_image_id    → original_image_id
    thumbnailImageId → thumbnail_image_id   → thumbnail_image_id
    largeImageId     → large_image_id       → large_image_id
    otherImageIds    → other_image_id       → other_image_ids
    createdAt        → created_at           → created_at
    updatedAt        → updated_at           → updated_at
    """
    item_id: str
    name: str
    brand: str | None = None
    model_number: str | None = None
    product_type: str | None = None
    country: str | None = None
    colors: list[str] = field(default_factory=list)
    styles: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    bullet_points: list[str] = field(default_factory=list)
    original_image_id: str | None = None
    thumbnail_image_id: str | None = None
    large_image_id: str | None = None
    other_image_ids: list[str] = field(default_factory=list)
    created_at: Any = None
    updated_at: Any = None

    def as_dict(self) -> dict:
        return {
            "item_id": self.item_id,
            "name": self.name,
            "brand": self.brand,
            "model_number": self.model_number,
            "product_type": self.product_type,
            "country": self.country,
            "colors": self.colors,
            "styles": self.styles,
            "keywords": self.keywords,
            "bullet_points": self.bullet_points,
            "original_image_id": self.original_image_id,
            "thumbnail_image_id": self.thumbnail_image_id,
            "large_image_id": self.large_image_id,
            "other_image_ids": self.other_image_ids,
            "created_at": str(self.created_at) if self.created_at else None,
            "updated_at": str(self.updated_at) if self.updated_at else None,
        }


# ---------------------------------------------------------------------------
# Internal helper: row dict → Product dataclass
# ---------------------------------------------------------------------------

def _row_to_product(row: dict) -> Product:
    return Product(
        item_id=row["item_id"],
        name=row["item_name"],
        brand=row.get("brand"),
        model_number=row.get("model_number"),
        product_type=row.get("product_type"),
        country=row.get("country"),
        colors=row.get("colors") or [],
        styles=row.get("styles") or [],
        keywords=row.get("keywords") or [],
        bullet_points=row.get("bullet_point") or [],
        original_image_id=row.get("original_image_id"),
        thumbnail_image_id=row.get("thumbnail_image_id"),
        large_image_id=row.get("large_image_id"),
        other_image_ids=row.get("other_image_id") or [],
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )


# ---------------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------------

def upsert_product(
    item_id: str,
    name: str,
    brand: str | None = None,
    model_number: str | None = None,
    product_type: str | None = None,
    country: str | None = None,
    colors: list[str] | None = None,
    styles: list[str] | None = None,
    keywords: list[str] | None = None,
    bullet_points: list[str] | None = None,
) -> Product:
    """
    Insert a new product or update all fields if item_id already exists.
    Image fields are NOT touched here — use link_product_images() for that.
    """
    client = PostgresClient.get_instance()
    sql = """
        INSERT INTO products (
            item_id, item_name, brand, model_number, product_type, country,
            colors, styles, keywords, bullet_point
        ) VALUES (
            %(item_id)s, %(name)s, %(brand)s, %(model_number)s,
            %(product_type)s, %(country)s,
            %(colors)s, %(styles)s, %(keywords)s, %(bullet_points)s
        )
        ON CONFLICT (item_id) DO UPDATE SET
            item_name     = EXCLUDED.item_name,
            brand         = EXCLUDED.brand,
            model_number  = EXCLUDED.model_number,
            product_type  = EXCLUDED.product_type,
            country       = EXCLUDED.country,
            colors        = EXCLUDED.colors,
            styles        = EXCLUDED.styles,
            keywords      = EXCLUDED.keywords,
            bullet_point  = EXCLUDED.bullet_point
        RETURNING *
    """
    params = {
        "item_id": item_id,
        "name": name,
        "brand": brand,
        "model_number": model_number,
        "product_type": product_type,
        "country": country,
        "colors": colors or [],
        "styles": styles or [],
        "keywords": keywords or [],
        "bullet_points": bullet_points or [],
    }
    with client.pool.connection() as conn:
        row = conn.execute(sql, params).fetchone()
        conn.commit()

    logger.info("Upserted product item_id='%s'", item_id)
    return _row_to_product(row)


def get_product(item_id: str) -> Product | None:
    """Fetch a single product by its item_id. Returns None if not found."""
    client = PostgresClient.get_instance()
    sql = "SELECT * FROM products WHERE item_id = %s"
    with client.pool.connection() as conn:
        row = conn.execute(sql, (item_id,)).fetchone()

    if row is None:
        return None
    return _row_to_product(row)


def list_products(
    limit: int = 20,
    offset: int = 0,
    brand: str | None = None,
    product_type: str | None = None,
) -> list[Product]:
    """
    Return a paginated list of products.
    Optionally filter by brand and/or product_type.
    """
    client = PostgresClient.get_instance()

    filters: list[str] = []
    params: dict = {"limit": limit, "offset": offset}

    if brand:
        filters.append("brand = %(brand)s")
        params["brand"] = brand
    if product_type:
        filters.append("product_type = %(product_type)s")
        params["product_type"] = product_type

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"""
        SELECT * FROM products
        {where_clause}
        ORDER BY created_at DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    with client.pool.connection() as conn:
        rows = conn.execute(sql, params).fetchall()

    return [_row_to_product(row) for row in rows]


def delete_product(item_id: str) -> bool:
    """
    Delete a product by its item_id.
    Returns True if a row was deleted, False if item_id was not found.
    """
    client = PostgresClient.get_instance()
    sql = "DELETE FROM products WHERE item_id = %s"
    with client.pool.connection() as conn:
        result = conn.execute(sql, (item_id,))
        conn.commit()

    deleted = result.rowcount > 0
    if deleted:
        logger.info("Deleted product item_id='%s'", item_id)
    else:
        logger.warning("Delete called but item_id='%s' not found", item_id)
    return deleted


def link_product_images(
    item_id: str,
    original_image_id: str | None = None,
    thumbnail_image_id: str | None = None,
    large_image_id: str | None = None,
    other_image_ids: list[str] | None = None,
) -> Product | None:
    """
    Update only the image fields of an existing product.
    Call this after uploading images to MinIO.
    Returns the updated Product, or None if item_id does not exist.
    """
    client = PostgresClient.get_instance()
    sql = """
        UPDATE products SET
            original_image_id   = %(original_image_id)s,
            thumbnail_image_id  = %(thumbnail_image_id)s,
            large_image_id      = %(large_image_id)s,
            other_image_id      = %(other_image_ids)s
        WHERE item_id = %(item_id)s
        RETURNING *
    """
    params = {
        "item_id": item_id,
        "original_image_id": original_image_id,
        "thumbnail_image_id": thumbnail_image_id,
        "large_image_id": large_image_id,
        "other_image_ids": other_image_ids or [],
    }
    with client.pool.connection() as conn:
        row = conn.execute(sql, params).fetchone()
        conn.commit()

    if row is None:
        logger.warning("link_product_images: item_id='%s' not found", item_id)
        return None

    logger.info("Linked images for product item_id='%s'", item_id)
    return _row_to_product(row)
