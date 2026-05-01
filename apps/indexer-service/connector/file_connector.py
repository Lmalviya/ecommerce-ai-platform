import os
import pandas as pd
from typing import Generator, Dict, Any
from ecom_image import ImageProcessor, ImageStorage

from .base import BaseConnector
from .cursor_store import CursorStore
from src.models import ProductDraft

class FileConnector(BaseConnector):
    """
    A universal file connector that handles CSV, Parquet, and Excel files.
    Uses Pandas for memory-efficient chunked reading.
    """

    def __init__(
        self, 
        connector_id: str, 
        cursor_store: CursorStore, 
        file_path: str,
        column_mapping: Dict[str, str]
    ):
        super().__init__(connector_id)
        self.file_path = file_path
        self.cursor_store = cursor_store
        self.mapping = column_mapping
        self.img_processor = self._init_image_processor()

    def fetch_items(self, chunk_size: int = 500) -> Generator[ProductDraft, None, None]:
        """Reads the file in chunks and yields mapped ProductDrafts."""
        
        # 1. Get last processed row index (Cursor)
        last_index = int(self.cursor_store.get_cursor(self.connector_id) or 0)
        
        # 2. Get the correct Pandas reader based on extension
        reader = self._get_reader(chunk_size)
        
        # 3. Iterate through chunks
        current_row = 0
        for chunk in reader:
            for index, row in chunk.iterrows():
                # Skip rows we've already processed
                if current_row < last_index:
                    current_row += 1
                    continue

                # 4. Map and Process
                draft = self._map_row_to_draft(row.to_dict())
                yield draft

                # 5. Save progress (Cursor is the row index)
                self.cursor_store.set_cursor(self.connector_id, str(current_row))
                current_row += 1

    def _get_storage_options(self) -> Optional[Dict[str, Any]]:
        """Returns credentials for Pandas to read from S3/MinIO."""
        if not self.file_path.startswith("s3://"):
            return None
            
        return {
            "key": os.getenv("SOURCE_MINIO_ACCESS_KEY", "admin"),
            "secret": os.getenv("SOURCE_MINIO_SECRET_KEY", "minio123"),
            "client_kwargs": {
                "endpoint_url": os.getenv("SOURCE_MINIO_ENDPOINT", "http://localhost:9000")
            }
        }

    def _get_reader(self, chunk_size: int):
        """Returns a Pandas reader based on file extension and storage protocol."""
        ext = os.path.splitext(self.file_path)[1].lower()
        storage_options = self._get_storage_options()
        
        if ext == '.csv':
            return pd.read_csv(
                self.file_path, 
                chunksize=chunk_size, 
                storage_options=storage_options
            )
        elif ext == '.parquet':
            # Note: Parquet via S3 requires storage_options in read_parquet
            df = pd.read_parquet(self.file_path, storage_options=storage_options)
            return [df[i:i+chunk_size] for i in range(0, df.shape[0], chunk_size)]
        elif ext in ['.xlsx', '.xls']:
            # Excel usually needs to be fully downloaded, but pandas handles s3:// urls
            df = pd.read_excel(self.file_path, storage_options=storage_options)
            return [df[i:i+chunk_size] for i in range(0, df.shape[0], chunk_size)]
        else:
            raise ValueError(f"Unsupported file format: {ext}")

    def _map_row_to_draft(self, row: Dict[str, Any]) -> ProductDraft:
        """Uses the column mapping to create a universal ProductDraft."""
        
        # Dynamic mapping logic
        def get_val(key, default=None):
            file_col_name = self.mapping.get(key)
            return row.get(file_col_name) if file_col_name else row.get(key, default)

        draft = ProductDraft(
            id=str(get_val("id", "no-id")),
            title=get_val("title"),
            description=get_val("description"),
            initial_price=float(get_val("initial_price", 0.0)),
            final_price=float(get_val("final_price", 0.0)),
            currency=get_val("currency", "USD"),
            availability=get_val("availability", "in-stock"),
            reviews_count=int(get_val("reviews_count", 0)),
            rating=float(get_val("rating", 0.0)),
            categories=get_val("categories", []),
            asin=get_val("asin"),
            seller_name=get_val("seller_name"),
            brand=get_val("brand"),
            original_image_url=get_val("image_url", ""),
            thumbnail_image_url="",
            features=get_val("features", []),
        )

        return self._process_image_for_draft(draft)

    