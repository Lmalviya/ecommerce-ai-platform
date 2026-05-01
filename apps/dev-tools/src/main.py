from fastapi import FastAPI
from routes.minio import image_router
from routes.postgres import metadata_router
from routes.qdrant import vector_router


app = FastAPI(
    title="Dev Tools API",
    description="Internal dev UI/API for managing Postgres, Qdrant, and MinIO",
    version="0.1.0"
)

# Mount the ecommerce storage router
app.include_router(image_router)
app.include_router(metadata_router)
app.include_router(vector_router)

@app.get("/health")
async def health_check():
    return {"status": "ok"}
