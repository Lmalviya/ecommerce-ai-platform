import logging
import asyncio
from celery import Task
from celery.signals import worker_process_init
from .celery_app import app

# Import our stages
from .stages.clean import ProductCleaner
from src.models import ProductDraft
from .orchestrator import IndexingPipeline
import requests

API_BASE_URL = "http://localhost:8000/api/v1"

# Singletons initialized once per worker process
cleaner = None
pipeline_orchestrator = None

@worker_process_init.connect
def init_worker(**kwargs):
    """
    Initialize heavy objects once per worker process.
    This prevents recreating AI connection pools per task.
    """
    global cleaner, pipeline_orchestrator
    logging.info("Initializing worker singletons...")
    cleaner = ProductCleaner()
    pipeline_orchestrator = IndexingPipeline()
    # We must ensure Qdrant collections exist before processing starts
    try:
        asyncio.run(pipeline_orchestrator.initialize_infrastructure())
    except Exception as e:
        logging.error(f"Failed to initialize Qdrant infra: {e}")

class PipelineTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logging.error(f"Task {task_id} failed: {exc}")

    def on_success(self, retval, task_id, args, kwargs):
        logging.info(f"Task {task_id} completed successfully")

def update_job_progress(job_id, processed_increment, failed_increment=0):
    if not job_id: return
    try:
        requests.patch(f"{API_BASE_URL}/jobs/{job_id}/progress", json={
            "processed_increment": processed_increment,
            "failed_increment": failed_increment
        })
    except Exception as e:
        logging.error(f"Failed to update job progress via API: {e}")

@app.task(base=PipelineTask, bind=True, name="process_product_batch")
def process_product_batch(self, products_data: list[dict], job_id: str = None):
    """
    Stage 2 & beyond: The Worker Task processing a BATCH of products.
    """
    drafts = []
    for data in products_data:
        try:
            draft = ProductDraft(**data)
            draft = cleaner.process(draft)
            drafts.append(draft)
        except Exception as e:
            logging.error(f"Cleaning failed for a product in batch: {e}")
            update_job_progress(job_id, 0, 1)

    # 3. Embed & Index
    summary = asyncio.run(pipeline_orchestrator.process_batch(drafts))
    
    # Update progress in DB
    update_job_progress(job_id, len(summary["successful_ids"]), len(summary["failed_ids"]))
    
    logging.info(f"Batch processed: {summary['total_processed']} items. Cost: {summary['total_cost']}")
    return summary
