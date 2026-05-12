# Connector Worker

The **Connector Worker** is the data-ingestion workhorse of the E-commerce AI Platform. It connects to external sources, fetches product data, and dispatches it to the indexing pipeline in manageable batches.

## 🚀 Quick Start

### 1. Run with Docker
```bash
docker compose up connector-worker --build
```

### 2. Scaling
To handle massive datasets faster, you can scale the number of worker containers:
```bash
docker compose up --scale connector-worker=3
```

## 🛠 Core Concepts

### 1. The Orchestrator Task
The worker listens for the `start_bulk_ingestion` task. This task:
1.  Initializes the correct **Connector** using the Factory Pattern.
2.  Streams items from the source.
3.  Groups items into **Batches** (default size: 50).
4.  Dispatches each batch to the **Indexer Worker** via the `process_product_batch` task.

### 2. Reliability & Retries
- **Retries**: If a source database or API is temporarily down, the worker will automatically retry the task up to 3 times with a 1-minute delay.
- **Structured Logging**: Uses `structlog` to output JSON logs. All logs are "bound" with a `job_id` for easy tracking in ELK/CloudWatch.

## 📂 Project Structure
- `src/tasks.py`: Celery task definitions and retry logic.
- `src/connectors/`:
    - `factory.py`: The entry point for creating connector instances.
    - `base.py`: The abstract interface all connectors must follow.
    - `postgres.py`: Connector for PostgreSQL sources.
    - `file_connector.py`: Connector for CSV/Parquet/S3 sources.
    - `cursor_store.py`: Logic for saving/loading sync bookmarks in Redis.

## 📖 Deep Dive
For technical details on extending the system or understanding the sync logic, see:
- [docs/CONNECTORS.md](./docs/CONNECTORS.md)
