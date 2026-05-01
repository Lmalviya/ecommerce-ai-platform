import os
from abc import ABC, abstractmethod
from typing import Generator, Optional, Any, Dict
from src.models import ProductDraft
from ecom_image import ImageProcessor, ImageStorage

class BaseConnector(ABC):

    def __init__(self, connector_id: str):
        self.connector_id = connector_id

    def _init_image_processor(self) -> ImageProcessor:
        """Shared helper to initialize the image processor from environment variables."""
        dest_storage = ImageStorage(
            endpoint=os.getenv("DEST_MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("DEST_MINIO_ACCESS_KEY", "admin"),
            secret_key=os.getenv("DEST_MINIO_SECRET_KEY", "minio123"),
            bucket=os.getenv("DEST_MINIO_BUCKET", "products")
        )

        source_storage = None
        source_endpoint = os.getenv("SOURCE_MINIO_ENDPOINT")
        if source_endpoint:
            source_storage = ImageStorage(
                endpoint=source_endpoint,
                access_key=os.getenv("SOURCE_MINIO_ACCESS_KEY"),
                secret_key=os.getenv("SOURCE_MINIO_SECRET_KEY"),
                bucket=os.getenv("SOURCE_MINIO_BUCKET")
            )

        return ImageProcessor(
            dest_storage=dest_storage,
            source_storage=source_storage
        )
    
    def _process_image_for_draft(self, draft: ProductDraft) -> ProductDraft:
        """Hand-off to the ImageProcessor facade."""
        source_url = draft.original_image_url
        if not source_url:
            return draft
            
        try:
            processed = self.img_processor.process_and_upload(
                url=source_url,
                product_id=draft.id
            )
            draft.original_image_url = processed['original_image_url']
            draft.thumbnail_image_url = processed['thumbnail_image_url']
        except Exception as e:
            print(f"❌ Failed to process image for product {draft.id}: {e}")
            
        return draft

    @abstractmethod
    def fetch_items(self, limit: int=100) -> Generator[ProductDraft, None, None]:
        """
        The core logic to pull data from the source
        """

    @abstractmethod
    def get_status(self) -> Dict:
        """
        Returns basic info about the connector status
        (e.g., last sync time, number of items processed).
        """

    