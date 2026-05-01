class ImageProcessorError(Exception):
    """Base exception for all image processing errors."""
    pass

class DownloadError(ImageProcessorError):
    """Raised when the image cannot be fetched from the source URL."""
    pass

class ImageValidationError(ImageProcessorError):
    """Raised when the image format is invalid or corrupted."""
    pass

class StorageUploadError(ImageProcessorError):
    """Raised when the upload to S3/Minio fails."""
    pass
