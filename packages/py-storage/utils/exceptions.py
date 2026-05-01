from __future__ import annotations


class MinioOperationError(Exception):
    """
    Raised when a MinIO S3 operation fails.

    Attributes
    ----------
    operation:
        Short label identifying which operation failed
        (e.g. ``"upload_image"``, ``"delete_zip"``).
    cause:
        The original exception that triggered this error, if any.
    """

    def __init__(
        self,
        operation: str,
        message: str,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(f"[MinIO:{operation}] {message}")
        self.operation = operation
        self.cause = cause
