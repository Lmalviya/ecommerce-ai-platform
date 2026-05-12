from typing import Dict, Type, Any
from .base import BaseConnector
from .postgres import PostgresConnector
from .file_connector import FileConnector
from .cursor_store import CursorStore

class ConnectorFactory:
    """True Factory Pattern: Standardized registry for creating connector instances."""
    
    # Registry mapping type names to classes
    _registry: Dict[str, Type[BaseConnector]] = {
        "postgres": PostgresConnector,
        "file": FileConnector,
    }

    @classmethod
    def get_connector(cls, connector_type: str, config: Dict[str, Any], cursor_store: CursorStore) -> BaseConnector:
        """
        Instantiates the appropriate connector based on type.
        NO if/else needed anymore because all connectors follow the same constructor contract!
        """
        connector_class = cls._registry.get(connector_type.lower())
        
        if not connector_class:
            raise ValueError(f"Unsupported connector type: {connector_type}")

        # Standardized instantiation
        return connector_class(config=config, cursor_store=cursor_store)
