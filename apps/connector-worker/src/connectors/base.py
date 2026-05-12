from abc import ABC, abstractmethod
from typing import Generator, Dict, Any
from src.models import ProductDraft
from .cursor_store import CursorStore

class BaseConnector(ABC):
    """Standardized base class for all data source connectors."""

    def __init__(self, config: Dict[str, Any], cursor_store: CursorStore):
        self.config = config
        self.cursor_store = cursor_store
        # Every config should at least have a unique ID for cursor tracking
        self.connector_id = config.get("id", "generic-connector")

    @abstractmethod
    def fetch_items(self, limit: int = 100) -> Generator[ProductDraft, None, None]:
        """The core logic to pull data from the source."""
        pass

    @abstractmethod
    def get_status(self) -> Dict[str, Any]:
        """Returns basic info about the connector status."""
        pass