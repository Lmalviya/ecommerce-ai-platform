import asyncio
import structlog
import httpx
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

from src.models import ProductDraft
from py_ai_models.clients.embedding import AIEmbedder
from py_ai_models.models.requests import EmbeddingRequest
from qdrant_store.client import AsyncQdrantStore
from qdrant_store.models import VectorParams, DistanceMetric, PayloadIndexType, VectorPoint, SparseVectorParams
from ecom_storage.repositories.product_repo import upsert_product, link_product_images

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
        
        # Semaphore limits concurrent async calls (API limits, memory, DB connections)
        self._semaphore = asyncio.Semaphore(self.config.max_concurrency)
        
    async def initialize_infrastructure(self):
        """Sets up Qdrant collections and indexes if they don't exist."""
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

    async def _download_image(self, client: httpx.AsyncClient, url: str) -> bytes | None:
        """Helper to download image for embedding via a shared HTTP client."""
        if not url:
            return None
        try:
            response = await client.get(url)
            response.raise_for_status()
            return response.content
        except Exception as e:
            logger.warning("Failed to download image", url=url, error=str(e))
            return None

    def _build_text_payload(self, draft: ProductDraft) -> str:
        cats = " > ".join(draft.categories)
        feats = " ".join(draft.features)
        return f"{draft.title} - {draft.brand}. Categories: {cats}. {draft.description or ''}. Features: {feats}"

    def _sync_to_postgres(self, draft: ProductDraft):
        """Wrapper for synchronous Postgres updates."""
        upsert_product(
            item_id=draft.id,
            name=draft.title,
            brand=draft.brand,
            colors=[],
            styles=[],
            keywords=draft.categories,
            bullet_points=draft.features
        )
        if draft.original_image_url or draft.thumbnail_image_url:
            link_product_images(
                item_id=draft.id,
                original_image_id=draft.original_image_url,
                thumbnail_image_id=draft.thumbnail_image_url
            )

    async def _process_single_draft(
        self, 
        draft: ProductDraft, 
        http_client: httpx.AsyncClient
    ) -> tuple[bool, str, Optional[VectorPoint], dict, str]:
        """
        Process a single draft inside a semaphore.
        Returns: (success, product_id, qdrant_point, cost_dict, error_msg)
        """
        cost = {"total_tokens": 0}
        
        async with self._semaphore:
            try:
                # 1. Text Embedding
                text_content = self._build_text_payload(draft)
                text_emb_response = await self.embedder.embed(
                    EmbeddingRequest(input=text_content, model=self.config.text_model)
                )
                text_vector = text_emb_response.embeddings[0]
                cost["total_tokens"] += text_emb_response.usage.total_tokens

                # 2. Image Embedding
                image_vector = None
                if draft.original_image_url:
                    img_bytes = await self._download_image(http_client, draft.original_image_url)
                    if img_bytes:
                        try:
                            img_emb_response = await self.embedder.embed(
                                EmbeddingRequest(input=img_bytes, model=self.config.image_model)
                            )
                            image_vector = img_emb_response.embeddings[0]
                        except Exception as e:
                            logger.error("Image embedding failed", product_id=draft.id, error=str(e))

                # 3. Store metadata in Postgres (offloaded to threadpool since it's sync)
                await asyncio.to_thread(self._sync_to_postgres, draft)

                # 4. Prepare Qdrant Vector Point
                vectors = {"text_dense": text_vector}
                if image_vector:
                    vectors["image_dense"] = image_vector
                    
                point = VectorPoint(
                    id=draft.id,
                    vectors=vectors,
                    payload=draft
                )
                
                return True, draft.id, point, cost, ""

            except Exception as e:
                logger.error("Failed to process draft", product_id=draft.id, error=str(e))
                return False, draft.id, None, cost, str(e)

    async def process_batch(self, drafts: List[ProductDraft]) -> Dict[str, Any]:
        """Main pipeline loop: Embed -> Postgres -> Qdrant"""
        summary = {
            "total_processed": len(drafts),
            "successful_ids": [],
            "failed_ids": {},
            "total_cost": {"total_tokens": 0}
        }
        
        qdrant_points = []
        
        # Shared connection pool for image downloads
        async with httpx.AsyncClient(timeout=self.config.request_timeout) as http_client:
            # Kick off all processing tasks concurrently
            tasks = [self._process_single_draft(draft, http_client) for draft in drafts]
            results = await asyncio.gather(*tasks)
            
        for success, product_id, point, cost, err_msg in results:
            summary["total_cost"]["total_tokens"] += cost["total_tokens"]
            if success:
                summary["successful_ids"].append(product_id)
                if point:
                    qdrant_points.append(point)
            else:
                summary["failed_ids"][product_id] = err_msg
                
        # 5. Bulk Upsert to Qdrant (still done sequentially at the batch level for efficiency)
        if qdrant_points:
            try:
                await self.qdrant.upsert_batch(self.config.collection_name, qdrant_points)
            except Exception as e:
                logger.error("Qdrant batch upsert failed", error=str(e))
                for pt in qdrant_points:
                    if pt.id in summary["successful_ids"]:
                        summary["successful_ids"].remove(pt.id)
                        summary["failed_ids"][pt.id] = f"Qdrant Upsert Error: {e}"

        return summary
