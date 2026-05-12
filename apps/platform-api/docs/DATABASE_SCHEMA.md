# Database Schema Documentation

This document describes the management tables used by the `platform-api`.

## Tables

### 1. `connector_profiles`
Stores the configuration for external data sources.

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | UUID | Primary Key (auto-generated). |
| `name` | TEXT | Human-readable name (e.g., "Main Shopify Store"). |
| `type` | TEXT | Type of connector (e.g., `shopify`, `csv`). |
| `config` | JSONB | Connector-specific configuration (API keys, file paths). |
| `created_at` | TIMESTAMPTZ | Creation timestamp. |

### 2. `ingestion_jobs`
Tracks the execution and progress of data sync operations.

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | UUID | Primary Key. |
| `profile_id` | UUID | Foreign Key to `connector_profiles(id)`. |
| `status` | TEXT | Current status (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`). |
| `total_items` | INTEGER | Total items discovered for processing. |
| `processed_items` | INTEGER | Number of items successfully indexed. |
| `failed_items` | INTEGER | Number of items that failed processing. |
| `error_message` | TEXT | Details if the job fails. |
| `created_at` | TIMESTAMPTZ | Start time. |
| `updated_at` | TIMESTAMPTZ | Last progress update. |

## Auto-Provisioning
The schema is defined in `apps/platform-api/src/main.py` and is executed automatically every time the server starts (`CREATE TABLE IF NOT EXISTS`).
