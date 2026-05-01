import os
import psycopg
from psycopg.rows import dict_row
from typing import Generator, Dict, Any, Optional
from ecom_image import ImageProcessor
from ecom_image.storage import ImageStorage

from .base import BaseConnector
from .cursor_store import CursorStore
from src.models import ProductDraft


class PostgresConnector(BaseConnector):
    """
    Connects to the postgresSQL products table and yields ProductDrafts
    using an incremental updated_at cursor.
    """

    def __init__(self, connector_id: str, cursor_store: CursorStore, db_url: str):
        super().__init__(connector_id)
        
        self.cursor_store = cursor_store
        self.db_url = db_url # The URL of the source database
        self._conn = None
        self.img_processor = self._init_image_processor()

    def _get_connection(self):
        """ Creates a connection to the source database """
        if self._conn is None or self._conn.closed:
            # we use dict_row so we can access columns by name: row["id"]
            self._conn = psycopg.connect(self.db_url, row_factory=dict_row)
        return self._conn 
        
    def fetch_items(self, limit: int = 100) -> Generator[ProductDraft, None, None]:
        """ Iterate over ProductDrafts from the source database """
        
        # 1. Get the bookmark from Redis
        cursor = self.cursor_store.get_cursor(self.connector_id)

        # If no bookmark exists, we start from the beginning of time
        if not cursor:
            cursor = "1970-01-01 00:00:00"

        # 2. Query the database for NEW or UPDATED rows
        query = """
            SELECT * FROM products 
            WHERE updated_at > %s 
            ORDER BY updated_at ASC 
            LIMIT %s
        """

        with self._get_connection() as conn:
            results = conn.execute(query, (cursor, limit)).fetchall()

            for row in results:
                # 3. Map DB row to universal ProductDraft format
                draft = self._map_row_to_draft(row)
                yield draft

                # 4. Update bookmark after every successful yield
                self.cursor_store.set_cursor(self.connector_id, str(row["updated_at"]))
    
    def _map_row_to_draft(self, row: Dict[str, Any]) -> ProductDraft:
        """Maps the DB product row to the ProductDraft DTO """
        draft = ProductDraft(
            id=str(row["id"]),
            title=row.get("title"),
            description=row.get("description"),
            initial_price=float(row.get("initial_price", 0.0)),
            final_price=float(row.get("final_price", 0.0)),
            currency=row.get("currency"),
            availability=row.get("availability"),
            reviews_count=int(row.get("reviews_count", 0)),
            rating=float(row.get("rating", 0.0)),
            categories=row.get("categories", []),
            asin=row.get("asin"),
            seller_name=row.get("seller_name"),
            brand=row.get("brand"),
            original_image_url=row.get("image_url"),
            thumbnail_image_url="",
            features=row.get("features", []),
        )

        return self._get_processed_image(draft)
                
            


                