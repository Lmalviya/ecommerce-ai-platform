import logging
from celery import Task
from .celery_app import app

# Import our stages and connectors
from .stages.clean import ProductCleaner
from src.models import ProductDraft
from connector.postgres import PostgresConnector
from connector.file_connector import FileConnector
from connector.cursor_store import CursorStore

# Lesson 8: Singleton Pattern for stages
# We initialize the cleaner ONCE at the top. 
# This way, the worker doesn't waste time recreating it for every product.
cleaner = ProductCleaner()

class PipelineTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logging.error(f"Task {task_id} failed: {exc}")

    def on_success(self, retval, task_id, args, kwargs):
        logging.info(f"Task {task_id} completed successfully")

@app.task(base=PipelineTask, bind=True, name="process_single_product")
def process_single_product(self, product_data: dict):
    """
    Stage 2 & beyond: The Worker Task.
    """
    # 1. Ingest: Convert raw dict back to our Python Model
    draft = ProductDraft(**product_data)
    
    # 2. Clean: Strip HTML and fix text
    draft = cleaner.process(draft)
    
    # Log progress
    logging.info(f"Cleaned product: {draft.title}")
    
    # Return the data as a dict for Celery to store in the backend
    return draft.model_dump()

@app.task(name="start_bulk_ingestion")
def start_bulk_ingestion(connector_type: str, config: dict):
    """
    Stage 1: The Orchestrator Task.
    """
    logging.info(f"Orchestrating bulk ingestion via {connector_type}")
    
    # 1. Setup the Cursor (The "Bookmark")
    cursor_store = CursorStore()
    
    # 2. Initialize the correct Connector
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

    # 3. Pull items and "Distribute" the work
    count = 0
    for draft in connector.fetch_items(limit=config.get("limit", 100)):
        # .delay() is the magic. It sends the product to the workers!
        process_single_product.delay(draft.model_dump())
        count += 1
    
    return {"status": "queued", "total_items": count}
