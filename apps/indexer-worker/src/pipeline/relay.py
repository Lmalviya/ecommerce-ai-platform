import asyncio
import structlog
import signal
import sys
from typing import List
from src.pipeline.orchestrator import IndexingPipeline
from src.pipeline.resilience import retry_internal_service
from src.repository import (
    fetch_pending_outbox_tasks, 
    mark_outbox_task_completed, 
    mark_outbox_task_failed
)

logger = structlog.get_logger(__name__)

class OutboxRelay:
    def __init__(self, batch_size: int = 20, poll_interval: float = 5.0):
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.pipeline = IndexingPipeline()
        self.running = True

    async def initialize(self):
        logger.info("Initializing Relay Infrastructure...")
        await self.pipeline.initialize_infrastructure()

    async def run(self):
        logger.info("Outbox Relay started", batch_size=self.batch_size, interval=self.poll_interval)
        
        while self.running:
            try:
                # 1. Fetch pending tasks from Postgres
                tasks = await asyncio.to_thread(fetch_pending_outbox_tasks, self.batch_size)
                
                if not tasks:
                    await asyncio.sleep(self.poll_interval)
                    continue

                logger.info("Processing outbox batch", count=len(tasks))
                
                # 2. Process each task (Embedding, Images)
                # We do this concurrently for the batch
                processing_tasks = []
                for task in tasks:
                    task_id, product_id, payload = task["id"], task["product_id"], task["payload"]
                    processing_tasks.append(self._handle_single_task(task_id, product_id, payload))
                
                # results will be a list of (task_id, VectorPoint or None, image_update or None)
                results = await asyncio.gather(*processing_tasks)
                
                # 3. Bulk Upsert to Qdrant (Resilient)
                qdrant_points = [point for _, point, _ in results if point is not None]
                image_updates = [update for _, _, update in results if update is not None]
                successful_task_ids = [task_id for task_id, point, _ in results if point is not None]

                if qdrant_points:
                    try:
                        @retry_internal_service
                        async def _upsert_bulk():
                            await self.pipeline.qdrant.upsert_batch(
                                self.pipeline.config.collection_name, 
                                qdrant_points
                            )
                        
                        await _upsert_bulk()

                        # 4. Bulk Update Postgres (Metadata & Status)
                        # We do these in two big batches instead of one-by-one
                        if image_updates:
                            await asyncio.to_thread(link_product_images_bulk, image_updates)
                        
                        await asyncio.to_thread(mark_outbox_tasks_completed_bulk, successful_task_ids)
                        
                        logger.info("Batch completed successfully", processed=len(successful_task_ids))
                    except Exception as e:
                        logger.error("Persistent Qdrant bulk upsert failure after retries", error=str(e))
                        # Mark all as failed for retry if Qdrant fails
                        for task_id in successful_task_ids:
                            await asyncio.to_thread(mark_outbox_task_failed, task_id, f"Qdrant Error: {e}")

            except Exception as e:
                logger.error("Relay loop error", error=str(e))
                await asyncio.sleep(self.poll_interval)

    async def _handle_single_task(self, task_id: int, product_id: str, payload: dict):
        """
        Processes a single outbox task. 
        Returns (task_id, VectorPoint, image_update)
        """
        try:
            point, image_update = await self.pipeline.process_outbox_task(product_id, payload)
            return task_id, point, image_update
        except Exception as e:
            logger.error("Task processing failed", product_id=product_id, error=str(e))
            await asyncio.to_thread(mark_outbox_task_failed, task_id, str(e))
            return task_id, None, None

    def stop(self):
        self.running = False
        self.pipeline.shutdown()

async def main():
    relay = OutboxRelay()
    
    # Handle graceful shutdown
    def signal_handler():
        logger.info("Shutdown signal received...")
        relay.stop()
        
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    await relay.initialize()
    await relay.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
