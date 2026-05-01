import io
from PIL import Image
from .exceptions import ImageValidationError

class ImageEngine:
    def __init__(self, quality: int = 85, thumb_size: tuple = (256, 256)):
        self.allowed_formats = {"JPEG", "JPG", "PNG", "WEBP"}
        self.quality = quality
        self.thumb_size = thumb_size

    def process(self, img_bytes: bytes) -> dict:
        """Converts to JPEG and creates a thumbnail."""
        try:
            img = Image.open(io.BytesIO(img_bytes))

            if img.format not in self.allowed_formats:
                raise ImageValidationError(f"Unsupported format: {img.format}. Allowed: {', '.join(self.allowed_formats)}")
            
            # Convert to RGB (Required for JPEG)
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")

            # 1. Standardize Original to JPEG
            orig_io = io.BytesIO()
            img.save(orig_io, format="JPEG", quality=self.quality)

            # 2. Create Thumbnail
            thumb = img.copy()
            thumb.thumbnail(self.thumb_size)
            thumb_io = io.BytesIO()
            thumb.save(thumb_io, format="JPEG", quality=self.quality - 10)

            return {
                "original": orig_io.getvalue(),
                "thumbnail": thumb_io.getvalue(),
                "format": "JPEG"
            }
        except Exception as e:
            raise ImageValidationError(f"Invalid image data: {str(e)}")
