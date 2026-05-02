import logging
from celery import Task
from .celery_app import app
from src.connectors.postgres import PostgresConnector
from src.connectors.file_connector import FileConnector
from src.connectors.cursor_store import CursorStore
import requests

API_BASE_URL = "http://localhost:8000/api/v1"

def set_job_status(job_id, status, total_items=None, error_message=None):
    if not job_id: return
    payload = {"status": status}
    if total_items is not None:
        payload["total_items"] = total_items
    if error_message:
        payload["error_message"] = error_message
    
    try:
        requests.patch(f"{API_BASE_URL}/jobs/{job_id}/status", json=payload)
    except Exception as e:
        logging.error(f"Failed to update job status via API: {e}")

class ConnectorTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logging.error(f"Task {task_id} failed: {exc}")

@app.task(base=ConnectorTask, bind=True, name="start_bulk_ingestion")
def start_bulk_ingestion(self, connector_type: str, config: dict, job_id: str = None, batch_size: int = 50, limit: int = 1000):
    """
    Stage 1: The Orchestrator Task for reading data from external sources.
    """
    logging.info(f"Orchestrating bulk ingestion via {connector_type} for job {job_id}")
    set_job_status(job_id, "RUNNING")
    
    try:
        cursor_store = CursorStore()
        
        if connector_type == "postgres":
            connector = PostgresConnector(
                connector_id=config.get("id", "pg-main"),
                cursor_store=cursor_store,
                db_url=config.get("db_url")
            )
        elif connector_type == "file":
            connector = FileConnector(
                connector_id=config.get("id", "file-main"),
                cursor_store=cursor_store,
                file_path=config.get("file_path"),
                column_mapping=config.get("mapping", {})
            )
        else:
            raise ValueError(f"Unknown connector type: {connector_type}")

        items = connector.fetch_items(limit=limit)
        
        total_queued = 0
        current_batch = []
        
        for draft in items:
            current_batch.append(draft.model_dump())
            if len(current_batch) >= batch_size:
                # We use send_task because process_product_batch is in another worker codebase!
                app.send_task("process_product_batch", args=[current_batch], kwargs={"job_id": job_id})
                total_queued += len(current_batch)
                current_batch = []
                
        if current_batch:
            app.send_task("process_product_batch", args=[current_batch], kwargs={"job_id": job_id})
            total_queued += len(current_batch)
        
        set_job_status(job_id, "RUNNING", total_items=total_queued)
        return {"status": "queued", "total_items": total_queued}

    except Exception as e:
        logging.error(f"Bulk ingestion failed: {e}")
        set_job_status(job_id, "FAILED", error_message=str(e))
        raise
