import redis
import logging
from typing import Optional

class CursorStore:
    def __init__(self, host: str="localhost", port: int=6379, db: int=0):
        self._redis = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self._prefix = "indexer:cursor:"

    def get_cursor(self, connector_id: str) -> Optional[str]:
        """ Retrive the last saved checkpoint for a connector """
        key = f"{self._prefix}{connector_id}"
        return self._redis.get(key)

    def set_cursor(self, connector_id: str, value: str):
        """ save a new checkpoint """
        key = f"{self._prefix}{connector_id}"
        self._redis.set(key, value)

    def clear_cursor(self, connector_id: str):
        """ delete a checkpoint (useful for re-indexing from scratch) """
        key = f"{self._prefix}{connector_id}"
        self._redis.delete(key)
