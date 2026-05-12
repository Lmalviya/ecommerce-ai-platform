# Connector Worker Architecture

The **Connector Worker** is the data acquisition engine of the platform. It is responsible for Stage 1 of the pipeline: pulling raw data from external sources and transforming it into a standardized format.

## The Factory Pattern
We use a **True Factory Pattern** to manage different data sources. This allows the worker to be highly extensible.

### How it works:
1.  The worker receives a task from Celery containing a `connector_type` (e.g., `"postgres"`) and a `config` dictionary.
2.  The `ConnectorFactory` looks up the corresponding class in its registry.
3.  The factory instantiates the class, passing the `config` directly to the connector.

### Standardized Interface
All connectors must inherit from `BaseConnector` and implement:
- `__init__(config, cursor_store)`: Setup connection details.
- `fetch_items(limit)`: A generator that yields `ProductDraft` objects.

---

## Cursor Management (Incremental Sync)
To avoid downloading the same data every time, we use a **Cursor (Bookmark)** system.

1.  **Storage**: Cursors are stored in Redis via the `CursorStore` class.
2.  **Mechanism**:
    - For **PostgreSQL**: We store the `updated_at` timestamp of the last processed row.
    - For **Files**: We store the row index (integer) of the last processed line.
3.  **Stateful Ingestion**: When a new job starts, the connector fetches its specific cursor from Redis and only requests data *after* that point.

---

## Adding a New Connector (Tutorial)
To add a new data source (e.g., `Shopify`):

1.  **Create the class**: In `src/connectors/shopify.py`, create a class inheriting from `BaseConnector`.
2.  **Implement `fetch_items`**: Add the logic to talk to the Shopify API.
3.  **Register it**: Add the new class to the `_registry` in `src/connectors/factory.py`.
    ```python
    _registry = {
        "postgres": PostgresConnector,
        "file": FileConnector,
        "shopify": ShopifyConnector, # Add this
    }
    ```
4.  **Update Dependencies**: If you need new libraries (like `shopifyapi`), add them to `pyproject.toml`.
