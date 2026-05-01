import json
import os
import tempfile
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from ecom_storage.configs.minio_config import ImageVariant
from ecom_storage.repositories.image_repo import (
    ProductImageURLs,
    delete_product_image,
    delete_product_images,
    delete_temp_file,
    download_product_image,
    download_temp_file,
    upload_product_image,
    # upload_product_images,
    upload_temp_file,
)
from utils.exceptions import MinioOperationError

image_router = APIRouter(prefix="/storage/image", tags=["image-storage"])

# ---------------------------------------------------------------------------
# Upload Operations
# ---------------------------------------------------------------------------

@image_router.post("/product/upload", response_model=dict)
async def api_upload_product_image(
    file: UploadFile = File(...),
    item_id: str = Form(...),
    category: str = Form(...)
):
    """Upload a single product image and generate its variants."""
    try:
        # Create a temporary file to store the upload before processing
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename or "").suffix) as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        try:
            urls: ProductImageURLs = upload_product_image(
                file_path=tmp_path,
                item_id=item_id,
                category=category
            )
            return urls.as_dict()
        finally:
            # Clean up the temporary file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    except MinioOperationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# @router.post("/products/upload-batch", response_model=List[dict])
# async def api_upload_product_images_batch(
#     files: List[UploadFile] = File(...),
#     metadata_json: str = Form(..., description="JSON string mapping filenames to {'item_id': str, 'category': str}")
# ):
#     """Batch upload multiple product images."""
#     try:
#         metadata = json.loads(metadata_json)
        
#         items_to_process = []
#         temp_files = []
        
#         try:
#             # Save all files to temp directory
#             for file in files:
#                 filename = file.filename
#                 if filename not in metadata:
#                     raise HTTPException(status_code=400, detail=f"Metadata missing for file: {filename}")
                
#                 item_meta = metadata[filename]
                
#                 fd, tmp_path = tempfile.mkstemp(suffix=Path(filename or "").suffix)
#                 with os.fdopen(fd, 'wb') as tmp:
#                     tmp.write(await file.read())
                
#                 temp_files.append(tmp_path)
                
#                 items_to_process.append({
#                     "file_path": tmp_path,
#                     "item_id": item_meta["item_id"],
#                     "category": item_meta["category"]
#                 })
            
#             # Process all files
            # results = upload_product_images(items_to_process)
#             return [res.as_dict() for res in results]
            
#         finally:
#             # Clean up all temporary files
#             for tmp_path in temp_files:
#                 if os.path.exists(tmp_path):
#                     os.unlink(tmp_path)

#     except json.JSONDecodeError:
#         raise HTTPException(status_code=400, detail="Invalid JSON in metadata_json")
#     except MinioOperationError as e:
#         raise HTTPException(status_code=400, detail=str(e))
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


@image_router.post("/directory/upload")
async def api_upload_temp_file(
    file: UploadFile = File(...),
    object_name: Optional[str] = Form(None)
):
    """Upload a raw file to the temporary bucket."""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename or "").suffix) as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        try:
            # Fallback to filename if object_name is not provided
            final_object_name = object_name or file.filename
            
            presigned_url = upload_temp_file(
                file_path=tmp_path,
                object_name=final_object_name
            )
            return {
                "message": "File uploaded successfully",
                "object_name": final_object_name,
                "presigned_url": presigned_url
            }
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    except MinioOperationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Download Operations
# ---------------------------------------------------------------------------

@image_router.get("/products/download/{category}/{item_id}")
async def api_download_product_image(
    category: str,
    item_id: str,
    variant: ImageVariant = ImageVariant.ORIGINAL
):
    """Download a product image variant."""
    try:
        image_bytes = download_product_image(
            item_id=item_id,
            category=category,
            variant=variant
        )
        
        # Stream the bytes back to the client
        def iterfile():
            yield image_bytes
            
        return StreamingResponse(iterfile(), media_type="image/jpeg")

    except MinioOperationError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@image_router.get("/directory/download")
async def api_download_temp_file(
    url_or_key: str
):
    """Download a temporary file to disk and serve it."""
    try:
        # download_temp_file saves to a temporary directory
        dest_path = download_temp_file(url_or_key=url_or_key)
        
        # FileResponse will stream the file and we can use background task to delete it if needed,
        # but the dev-tools environment can also just leave it in /tmp or clean it up periodically.
        return FileResponse(
            path=dest_path, 
            filename=dest_path.name,
            media_type="application/octet-stream"
        )
        
    except MinioOperationError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Delete Operations
# ---------------------------------------------------------------------------

@image_router.delete("/products/{category}/{item_id}")
async def api_delete_product_image(
    category: str,
    item_id: str,
    variant: Optional[ImageVariant] = None
):
    """Delete one or all variants of a product image."""
    try:
        delete_product_image(
            item_id=item_id,
            category=category,
            variant=variant
        )
        return {"message": "Product image(s) deleted successfully"}
    except MinioOperationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class DeleteBatchItem(BaseModel):
    item_id: str
    category: str

@image_router.delete("/products/batch")
async def api_delete_product_images_batch(
    items: List[DeleteBatchItem]
):
    """Batch delete multiple product images (all variants)."""
    try:
        items_dict = [{"item_id": item.item_id, "category": item.category} for item in items]
        delete_product_images(items_dict)
        return {"message": "Batch deletion completed successfully"}
    except MinioOperationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@image_router.delete("/file")
async def api_delete_temp_file(
    url_or_key: str
):
    """Delete a temporary file."""
    try:
        delete_temp_file(url_or_key=url_or_key)
        return {"message": "Temporary file deleted successfully"}
    except MinioOperationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
