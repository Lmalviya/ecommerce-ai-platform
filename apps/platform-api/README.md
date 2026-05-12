# Platform API

The **Platform API** is the central management service (Control Plane) for the E-commerce AI Platform. It provides RESTful endpoints to manage data connectors and orchestrate background ingestion jobs.

## 🚀 Quick Start

### 1. Run with Docker (Recommended)
From the project root:
```bash
docker compose up platform-api --build
```

### 2. Run Locally (Development)
Ensure you have `uv` installed and a PostgreSQL/Redis instance running.
```bash
cd apps/platform-api
uv sync
uv run uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

## 🛠 Features
- **Connector Management**: Register and configure sources like Shopify, Amazon, or local files.
- **Multi-Source Support**: Register unlimited unique data sources (e.g., multiple Postgres servers) and trigger jobs for each individually.
- **Job Orchestration**: Trigger and monitor bulk data ingestion tasks.
- **Structured Logging**: JSON-based logging for production observability.
- **Auto-Provisioning**: Automatically creates required database tables on startup.

## 📂 Project Structure
- `src/main.py`: App entry point and schema initialization.
- `src/api/`:
    - `endpoints.py`: REST API route handlers.
    - `schemas.py`: Pydantic models for request/response validation.
- `src/database.py`: PostgreSQL connection pooling logic.
- `src/celery_app.py`: Background task configuration.

## 📖 Documentation
- **Interactive API Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Deep-Dive Architecture**: See [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md)

## ⚠️ Database SQL Policy
This service currently uses **Raw SQL** for data access to minimize abstraction overhead during the build phase. All queries MUST use parameterization to prevent SQL injection. In the future, this may be migrated to SQLAlchemy/Alembic for better schema versioning.
