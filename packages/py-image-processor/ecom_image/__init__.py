from typing import Optional
from .downloader import ImageDownloader
from .engine import ImageEngine
from .storage import ImageStorage
from .exceptions import ImageProcessorError

class ImageProcessor:
    """
    Production-grade Image Processor.
    Orchestrates downloading from Web or S3, transforming, and uploading.
    """
    def __init__(self, dest_storage: ImageStorage, source_storage: Optional[ImageStorage] = None):
        """
        Args:
            dest_storage: Where the processed JPEGs will be saved.
            source_storage: Optional. Used if the product image URLs point to a private S3/MinIO bucket.
        """
        self.dest = dest_storage
        self.source = source_storage
        
        self.web_downloader = ImageDownloader()
        self.engine = ImageEngine()

    def process_and_upload(self, url: str, product_id: str) -> dict:
        """
        Full pipeline: Fetch -> Transform -> Upload.
        """
        # 1. SMART DOWNLOAD
        # If it looks like an S3 URI or we have a source storage configured
        if url.startswith("s3://") or (self.source and not url.startswith("http")):
            if not self.source:
                raise ImageProcessorError("S3 URL provided but no source_storage configured")
            img_bytes = self.source.download(url)
        else:
            # Standard HTTP/HTTPS download
            img_bytes = self.web_downloader.download(url)

        # 2. TRANSFORM
        variants = self.engine.process(img_bytes)

        # 3. UPLOAD TO DESTINATION
        orig_path = f"products/{product_id}/original.jpg"
        thumb_path = f"products/{product_id}/thumb.jpg"

        orig_url = self.dest.upload(variants['original'], orig_path)
        thumb_url = self.dest.upload(variants['thumbnail'], thumb_path)

        return {
            "original_image_url": orig_url,
            "thumbnail_image_url": thumb_url
        }
