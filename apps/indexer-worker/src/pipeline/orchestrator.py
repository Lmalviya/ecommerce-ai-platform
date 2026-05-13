import asyncio
import structlog
import httpx
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

import os
from src.models import ProductDraft
from py_ai_models.clients.embedding import AIEmbedder
from py_ai_models.models.requests import EmbeddingRequest
from qdrant_store.client import AsyncQdrantStore
from qdrant_store.models import VectorParams, DistanceMetric, PayloadIndexType, VectorPoint, SparseVectorParams
from src.repository import ingest_product_atomic, link_product_images
from ecom_image import ImageProcessor, ImageStorage
from .resilience import retry_ai_api, retry_internal_service
from .cpu_worker import process_image_cpu_task

logger = structlog.get_logger(__name__)

class PipelineConfig(BaseModel):
    """Configuration for the Indexing Pipeline."""
    collection_name: str = Field(default="products")
    text_model: str = Field(default="text-embedding-3-small", description="Model for text embeddings")
    image_model: str = Field(default="clip-vit-base-patch32", description="Model for image embeddings")
    text_vector_size: int = Field(default=1536)
    image_vector_size: int = Field(default=512)
    max_concurrency: int = Field(default=20, description="Max concurrent processing tasks (Semaphore limit)")
    use_quantization: bool = Field(default=True, description="Use Qdrant Scalar Quantization")
    request_timeout: float = Field(default=10.0, description="Timeout for external HTTP requests")


class IndexingPipeline:
    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        
        # Injectable components
        self.embedder = AIEmbedder()
        self.qdrant = AsyncQdrantStore()
        
        # Initialize Image Processor
        dest_storage = ImageStorage(
            endpoint=os.getenv("DEST_MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("DEST_MINIO_ACCESS_KEY", "admin"),
            secret_key=os.getenv("DEST_MINIO_SECRET_KEY", "minio123"),
            bucket=os.getenv("DEST_MINIO_BUCKET", "products")
        )
        self.img_processor = ImageProcessor(dest_storage=dest_storage)
        
        # Process Pool for CPU-bound image resizing
        # We use num_cpus - 1 to keep a core free for the main async loop
        max_workers = max(1, multiprocessing.cpu_count() - 1)
        self.executor = ProcessPoolExecutor(max_workers=max_workers)
        
        # Semaphore limits concurrent async calls (API limits, memory, DB connections)
        self._semaphore = asyncio.Semaphore(self.config.max_concurrency)
        
    async def initialize_infrastructure(self):
        """Sets up Qdrant collections and indexes if they don't exist."""
        
        @retry_internal_service
        async def _create_coll():
            vectors_config = {
                "text_dense": VectorParams(
                    size=self.config.text_vector_size, 
                    distance=DistanceMetric.COSINE, 
                    use_scalar_quantization=self.config.use_quantization
                ),
                "image_dense": VectorParams(
                    size=self.config.image_vector_size, 
                    distance=DistanceMetric.COSINE, 
                    use_scalar_quantization=self.config.use_quantization
                )
            }
            
            sparse_config = {
                "text_sparse": SparseVectorParams()
            }
            
            logger.info("Initializing Qdrant Collection...", collection=self.config.collection_name)
            await self.qdrant.create_collection(
                name=self.config.collection_name,
                vectors_config=vectors_config,
                sparse_vectors_config=sparse_config
            )

        await _create_coll()
        
        indexes = {
            "brand": PayloadIndexType.KEYWORD,
            "categories": PayloadIndexType.KEYWORD,
            "final_price": PayloadIndexType.FLOAT,
            "rating": PayloadIndexType.FLOAT,
            "availability": PayloadIndexType.KEYWORD
        }
        
        for field, index_type in indexes.items():
            await self.qdrant.create_payload_index(self.config.collection_name, field, index_type)
            
        logger.info("Qdrant infrastructure ready.")

    def _build_text_payload(self, draft: ProductDraft) -> str:
        cats = " > ".join(draft.categories)
        feats = " ".join(draft.features)
        return f"{draft.title} - {draft.brand}. Categories: {cats}. {draft.description or ''}. Features: {feats}"

    async def ingest_to_outbox(self, drafts: List[ProductDraft]) -> Dict[str, Any]:
        """
        Fast path: Validates and saves to Postgres Outbox. 
        """
        success_ids = []
        failed_ids = {}
        
        for draft in drafts:
            try:
                # Offload sync DB call to thread
                await asyncio.to_thread(ingest_product_atomic, draft.model_dump())
                success_ids.append(draft.id)
            except Exception as e:
                logger.error("Failed to ingest to outbox", product_id=draft.id, error=str(e))
                failed_ids[draft.id] = str(e)
                
        return {
            "total_processed": len(drafts),
            "successful_ids": success_ids,
            "failed_ids": failed_ids,
            "total_cost": {"total_tokens": 0}
        }

    @retry_ai_api
    async def _embed_text_resilient(self, draft: ProductDraft):
        text_content = self._build_text_payload(draft)
        return await self.embedder.embed(
            EmbeddingRequest(input=text_content, model=self.config.text_model)
        )

    @retry_ai_api
    async def _embed_image_resilient(self, img_bytes: bytes):
        return await self.embedder.embed(
            EmbeddingRequest(input=img_bytes, model=self.config.image_model)
        )

    async def process_outbox_task(self, product_id: str, payload: dict) -> tuple[VectorPoint, Optional[dict]]:
        """
        The Heavy Path: Uses Process Pool for images and Async for AI.
        """
        draft = ProductDraft(**payload)
        
        # 1. Text Embedding (Async/IO)
        text_emb_response = await self._embed_text_resilient(draft)
        text_vector = text_emb_response.embeddings[0]

        # 2. Image Processing & Embedding
        image_vector = None
        image_update = None
        
        if draft.original_image_url:
            try:
                # A. Download (Async/IO)
                img_bytes = await asyncio.to_thread(
                    self.img_processor.web_downloader.download, 
                    draft.original_image_url
                )

                # B. Resize (Parallel Process/CPU)
                loop = asyncio.get_running_loop()
                variants = await loop.run_in_executor(
                    self.executor, 
                    process_image_cpu_task, 
                    img_bytes
                )
                
                if "error" in variants:
                    raise Exception(f"Image transformation failed: {variants['error']}")

                # C. Upload (Async/IO)
                orig_path = f"products/{draft.id}/original.jpg"
                thumb_path = f"products/{draft.id}/thumb.jpg"
                
                orig_url = await asyncio.to_thread(self.img_processor.dest.upload, variants['original'], orig_path)
                thumb_url = await asyncio.to_thread(self.img_processor.dest.upload, variants['thumbnail'], thumb_path)

                # D. Metadata Update
                image_update = {
                    "id": draft.id,
                    "original": orig_url,
                    "thumb": thumb_url
                }
                draft.original_image_url = orig_url
                draft.thumbnail_image_url = thumb_url
                
                # E. Embed the Image (Async/IO)
                img_emb_response = await self._embed_image_resilient(img_bytes)
                image_vector = img_emb_response.embeddings[0]

            except Exception as e:
                logger.error("ProcessPool image processing failed", product_id=draft.id, error=str(e))
                pass

        # 3. Prepare Vector Point
        vectors = {"text_dense": text_vector}
        if image_vector:
            vectors["image_dense"] = image_vector
            
        point = VectorPoint(
            id=draft.id,
            vectors=vectors,
            payload=draft.model_dump()
        )
        
        return point, image_update

    async def process_batch(self, drafts: List[ProductDraft]) -> Dict[str, Any]:
        """
        Backward compatibility entry point.
        """
        return await self.ingest_to_outbox(drafts)

    def shutdown(self):
        """Clean up the process pool."""
        self.executor.shutdown()
