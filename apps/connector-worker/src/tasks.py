import os
import requests
import structlog
import logging
from celery import Task
from src.celery_app import app
from src.connectors.factory import ConnectorFactory
from src.connectors.cursor_store import CursorStore

# Setup structured logging
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

API_BASE_URL = os.getenv("API_BASE_URL", "http://platform-api:8000/api/v1")

def set_job_status(job_id: str, status: str, total_items: int = None, error_message: str = None):
    """Helper to update job status in the Platform API."""
    if not job_id:
        return
        
    payload = {"status": status}
    if total_items is not None:
        payload["total_items"] = total_items
    if error_message:
        payload["error_message"] = error_message
    
    try:
        requests.patch(f"{API_BASE_URL}/jobs/{job_id}/status", json=payload, timeout=5)
    except Exception as e:
        logger.error("api_status_update_failed", error=str(e), job_id=job_id)

class ConnectorTask(Task):
    """Base task class for common error handling and logging."""
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error("task_execution_failed", task_id=task_id, error=str(exc))

@app.task(
    base=ConnectorTask, 
    bind=True, 
    name="start_bulk_ingestion",
    max_retries=3,
    default_retry_delay=60  # Retry after 1 minute
)
def start_bulk_ingestion(self, connector_type: str, config: dict, job_id: str = None, batch_size: int = 50, limit: int = 1000):
    """
    Stage 1: The Orchestrator Task.
    Fetches data from external sources and dispatches batches to the indexing pipeline.
    """
    log = logger.bind(job_id=job_id, connector_type=connector_type)
    log.info("bulk_ingestion_started")
    
    set_job_status(job_id, "RUNNING")
    
    try:
        cursor_store = CursorStore()
        
        # Using the Factory Pattern to resolve the connector
        connector = ConnectorFactory.get_connector(
            connector_type=connector_type,
            config=config,
            cursor_store=cursor_store
        )

        items = connector.fetch_items(limit=limit)
        
        total_queued = 0
        current_batch = []
        
        for draft in items:
            current_batch.append(draft.model_dump())
            
            if len(current_batch) >= batch_size:
                app.send_task(
                    "process_product_batch", 
                    args=[current_batch], 
                    kwargs={"job_id": job_id},
                    queue="indexing" # Routing to specific queue
                )
                total_queued += len(current_batch)
                current_batch = []
                
        # Send remaining items
        if current_batch:
            app.send_task(
                "process_product_batch", 
                args=[current_batch], 
                kwargs={"job_id": job_id},
                queue="indexing"
            )
            total_queued += len(current_batch)
        
        # Final status update
        set_job_status(job_id, "RUNNING", total_items=total_queued)
        log.info("bulk_ingestion_dispatched", total_items=total_queued)
        
        return {"status": "queued", "total_items": total_queued}

    except Exception as e:
        log.error("bulk_ingestion_error", error=str(e))
        
        # If it's a transient error (like a network issue), retry the task
        if self.request.retries < self.max_retries:
            log.info("retrying_task", attempt=self.request.retries + 1)
            raise self.retry(exc=e)
        
        # Final failure reporting
        set_job_status(job_id, "FAILED", error_message=str(e))
        raise
