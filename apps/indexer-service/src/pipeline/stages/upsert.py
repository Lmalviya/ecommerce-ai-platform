import logging
import psycopg
import uuid
from typing import List
from src.models import ProductDraft
from qdrant_store import QdrantStore, VectorPoint, ProductPayload

class UpsertStage:
    """
    Stage 5: Upsert
    Saves the processed product to Postgres and Qdrant.
    """

    def __init__(self, pg_conn_url: str, qdrant_client: QdrantStore):
        self.pg_url = pg_conn_url
        self.qdrant = qdrant_client

    def _upsert_postgres(self, draft: ProductDraft):
        """Saves product metadata to Postgres."""
        query = """
            INSERT INTO products (
                id, title, description, final_price, currency, 
                image_url, thumbnail_url, brand, rating, reviews_count
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                final_price = EXCLUDED.final_price,
                image_url = EXCLUDED.image_url,
                thumbnail_url = EXCLUDED.thumbnail_url,
                updated_at = CURRENT_TIMESTAMP;
        """
        try:
            with psycopg.connect(self.pg_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (
                        draft.id, draft.title, draft.description, 
                        draft.final_price, draft.currency,
                        draft.original_image_url, draft.thumbnail_image_url,
                        draft.brand, draft.rating, draft.reviews_count
                    ))
        except Exception as e:
            logging.error(f"Postgres Upsert Failed for {draft.id}: {e}")
            raise

    def _upsert_qdrant(self, draft: ProductDraft, vector: List[float]):
        """Saves product vector and payload to Qdrant."""
        
        # 1. Create a deterministic UUID from the Product ID (ASIN)
        # Qdrant requires IDs to be valid UUIDs or Integers.
        point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, draft.id))

        # 2. Prepare the payload
        payload = ProductPayload(
            id=draft.id,
            title=draft.title,
            price=draft.final_price or 0.0,
            currency=draft.currency,
            image_url=draft.original_image_url or "",
            thumbnail_url=draft.thumbnail_image_url or "",
            brand=draft.brand,
            categories=draft.categories,
            rating=draft.rating,
            reviews_count=draft.reviews_count
        )

        # 3. Create the point
        point = VectorPoint(
            id=point_id,
            vector=vector,
            payload=payload
        )

        # 4. Upsert to Qdrant
        try:
            self.qdrant.upsert_batch(
                collection_name="products",
                points=[point]
            )
        except Exception as e:
            logging.error(f"Qdrant Upsert Failed for {draft.id}: {e}")
            raise

    def process(self, draft: ProductDraft, vector: List[float]):
        """Main entry point for the Upsert stage."""
        self._upsert_postgres(draft)
        self._upsert_qdrant(draft, vector)
        logging.info(f"Indexed product {draft.id}")
