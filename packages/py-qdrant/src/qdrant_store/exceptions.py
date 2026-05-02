class QdrantStoreError(Exception):
    """Base exception for Qdrant store errors."""
    pass

class CollectionNotFoundError(QdrantStoreError):
    """Raised when a requested collection does not exist."""
    pass

class QdrantConnectionError(QdrantStoreError):
    """Raised when the client cannot connect to the Qdrant server."""
    pass

class SearchError(QdrantStoreError):
    """Raised when a search operation fails."""
    pass
