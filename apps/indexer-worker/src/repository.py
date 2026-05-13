import logging
import json
from .database import DatabaseClient

logger = logging.getLogger(__name__)

def ingest_product_atomic(draft_dict: dict):
    """
    Atomic transaction to save product metadata and create an outbox task.
    """
    client = DatabaseClient.get_instance()
    
    product_sql = """
    INSERT INTO products (id, name, brand, colors, styles, keywords, bullet_points, updated_at)
    VALUES (%(id)s, %(name)s, %(brand)s, %(colors)s, %(styles)s, %(keywords)s, %(bullet_points)s, NOW())
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        brand = EXCLUDED.brand,
        colors = EXCLUDED.colors,
        styles = EXCLUDED.styles,
        keywords = EXCLUDED.keywords,
        bullet_points = EXCLUDED.bullet_points,
        updated_at = NOW();
    """
    
    outbox_sql = """
    INSERT INTO indexing_outbox (product_id, payload, status)
    VALUES (%s, %s, 'PENDING')
    """
    
    try:
        with client.pool.connection() as conn:
            with conn.transaction():
                # 1. Update metadata
                conn.execute(product_sql, {
                    "id": draft_dict["id"],
                    "name": draft_dict["title"],
                    "brand": draft_dict.get("brand"),
                    "colors": [],
                    "styles": [],
                    "keywords": draft_dict.get("categories", []),
                    "bullet_points": draft_dict.get("features", [])
                })
                
                # 2. Add to outbox
                conn.execute(outbox_sql, (draft_dict["id"], json.dumps(draft_dict)))
    except Exception as e:
        logger.error(f"Atomic ingestion failed for {draft_dict.get('id')}: {e}")
        raise

def fetch_pending_outbox_tasks(limit: int = 50):
    """
    Fetches tasks that are ready for processing.
    Ready = (PENDING or FAILED) AND (next_retry_at <= now) AND (retry_count < 5)
    """
    client = DatabaseClient.get_instance()
    sql = """
    UPDATE indexing_outbox
    SET status = 'PROCESSING'
    WHERE id IN (
        SELECT id FROM indexing_outbox
        WHERE status IN ('PENDING', 'FAILED')
        AND next_retry_at <= NOW()
        AND retry_count < 5
        ORDER BY created_at ASC
        LIMIT %s
        FOR UPDATE SKIP LOCKED
    )
    RETURNING id, product_id, payload;
    """
    try:
        with client.pool.connection() as conn:
            res = conn.execute(sql, (limit,))
            return res.fetchall()
    except Exception as e:
        logger.error(f"Failed to fetch outbox tasks: {e}")
        return []

def mark_outbox_tasks_completed_bulk(task_ids: list[int]):
    """
    Marks multiple tasks as completed in a single SQL call.
    """
    if not task_ids: return
    client = DatabaseClient.get_instance()
    sql = "UPDATE indexing_outbox SET status = 'COMPLETED', processed_at = NOW() WHERE id = ANY(%s)"
    try:
        with client.pool.connection() as conn:
            conn.execute(sql, (task_ids,))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to bulk mark tasks as completed: {e}")

def link_product_images_bulk(image_links: list[dict]):
    """
    Updates multiple product image URLs in a single transaction.
    Expects a list of dicts: [{'id': '...', 'original': '...', 'thumb': '...'}, ...]
    """
    if not image_links: return
    client = DatabaseClient.get_instance()
    sql = """
    UPDATE products 
    SET image_url = data.original, thumbnail_url = data.thumb 
    FROM (SELECT unnest(%s) as id, unnest(%s) as original, unnest(%s) as thumb) AS data
    WHERE products.id = data.id
    """
    
    ids = [item['id'] for item in image_links]
    originals = [item['original'] for item in image_links]
    thumbs = [item['thumb'] for item in image_links]
    
    try:
        with client.pool.connection() as conn:
            conn.execute(sql, (ids, originals, thumbs))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to bulk link images: {e}")

def mark_outbox_task_completed(task_id: int):
    client = DatabaseClient.get_instance()
    sql = "UPDATE indexing_outbox SET status = 'COMPLETED', processed_at = NOW() WHERE id = %s"
    try:
        with client.pool.connection() as conn:
            conn.execute(sql, (task_id,))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to mark task {task_id} as completed: {e}")

def mark_outbox_task_failed(task_id: int, error_msg: str):
    """
    Handles task failure with exponential backoff.
    If retry_count >= 5, marks as DEAD.
    """
    client = DatabaseClient.get_instance()
    # Backoff logic: 1min, 4min, 9min, 16min... (square of retry count)
    sql = """
    UPDATE indexing_outbox 
    SET 
        status = CASE WHEN retry_count >= 4 THEN 'DEAD' ELSE 'FAILED' END,
        retry_count = retry_count + 1,
        next_retry_at = NOW() + (INTERVAL '1 minute' * pow(retry_count + 1, 2)),
        last_error = %s,
        processed_at = NOW()
    WHERE id = %s
    """
    try:
        with client.pool.connection() as conn:
            conn.execute(sql, (error_msg, task_id))
            conn.commit()
            
        logger.warning(f"Task {task_id} failed. Error recorded and backoff scheduled.")
    except Exception as e:
        logger.error(f"Failed to mark task as failed: {e}")

def upsert_product(
    item_id: str,
    name: str,
    brand: str = None,
    colors: list = None,
    styles: list = None,
    keywords: list = None,
    bullet_points: list = None
):
    """
    Upserts a product into the target ecommerce_data database.
    (Kept for backward compatibility)
    """
    client = DatabaseClient.get_instance()
    
    sql = """
    INSERT INTO products (id, name, brand, colors, styles, keywords, bullet_points, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        brand = EXCLUDED.brand,
        colors = EXCLUDED.colors,
        styles = EXCLUDED.styles,
        keywords = EXCLUDED.keywords,
        bullet_points = EXCLUDED.bullet_points,
        updated_at = NOW();
    """
    
    try:
        with client.pool.connection() as conn:
            conn.execute(sql, (
                item_id, name, brand, 
                colors or [], styles or [], 
                keywords or [], bullet_points or []
            ))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to upsert product {item_id}: {e}")
        raise

def link_product_images(item_id: str, original_image_id: str = None, thumbnail_image_id: str = None):
    """
    Links processed image IDs to the product.
    """
    client = DatabaseClient.get_instance()
    
    sql = """
    UPDATE products 
    SET image_url = %s, thumbnail_url = %s 
    WHERE id = %s
    """
    
    try:
        with client.pool.connection() as conn:
            conn.execute(sql, (original_image_id, thumbnail_image_id, item_id))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to link images for product {item_id}: {e}")
        raise
