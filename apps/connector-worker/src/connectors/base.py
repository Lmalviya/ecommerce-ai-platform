import os
from abc import ABC, abstractmethod
from typing import Generator, Optional, Any, Dict
from src.models import ProductDraft

class BaseConnector(ABC):

    def __init__(self, connector_id: str):
        self.connector_id = connector_id

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

    