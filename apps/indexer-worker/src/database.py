import os
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

# This is the TARGET database where products are stored after indexing
DATABASE_URL = os.getenv("TARGET_DB_URL", "postgresql://admin:admin123@localhost:5432/ecommerce_data")

class DatabaseClient:
    _instance = None

    def __init__(self):
        self._pool = ConnectionPool(
            conninfo=DATABASE_URL,
            min_size=1,
            max_size=10,
            kwargs={"row_factory": dict_row},
        )

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @property
    def pool(self):
        return self._pool

    def close(self):
        self._pool.close()
