import logging
import asyncio
from celery import Task
from celery.signals import worker_process_init
from .celery_app import app

# Import our stages and connectors
from .stages.clean import ProductCleaner
from src.models import ProductDraft
from connector.postgres import PostgresConnector
from connector.file_connector import FileConnector
from connector.cursor_store import CursorStore
from .orchestrator import IndexingPipeline

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

@app.task(base=PipelineTask, bind=True, name="process_product_batch")
def process_product_batch(self, products_data: list[dict]):
    """
    Stage 2 & beyond: The Worker Task processing a BATCH of products.
    """
    drafts = []
    for data in products_data:
        # 1. Ingest
        draft = ProductDraft(**data)
        # 2. Clean (from existing pipeline stages)
        draft = cleaner.process(draft)
        drafts.append(draft)
    
    # 3. Embed & Index (Async pipeline execution wrapped for Celery)
    summary = asyncio.run(pipeline_orchestrator.process_batch(drafts))
    
    logging.info(f"Batch processed: {summary['total_processed']} items. Cost: {summary['total_cost']}")
    return summary

@app.task(name="start_bulk_ingestion")
def start_bulk_ingestion(connector_type: str, config: dict):
    """
    Stage 1: The Orchestrator Task.
    """
    logging.info(f"Orchestrating bulk ingestion via {connector_type}")
    
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

    # 3. Pull items, Chunk them, and "Distribute" the work
    batch_size = config.get("batch_size", 50)
    limit = config.get("limit", 1000)
    
    items = connector.fetch_items(limit=limit)
    
    total_queued = 0
    current_batch = []
    
    # Process items in batches to optimize worker throughput
    for draft in items:
        current_batch.append(draft.model_dump())
        if len(current_batch) >= batch_size:
            # .delay() sends the batch to the workers!
            process_product_batch.delay(current_batch)
            total_queued += len(current_batch)
            current_batch = []
            
    # Flush the remainder
    if current_batch:
        process_product_batch.delay(current_batch)
        total_queued += len(current_batch)
    
    return {"status": "queued", "total_items": total_queued}
