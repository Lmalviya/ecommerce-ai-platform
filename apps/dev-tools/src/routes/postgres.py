from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ecom_storage.repositories.product_repo import (
    delete_product,
    get_product,
    link_product_images,
    list_products,
    upsert_product,
)

metadata_router = APIRouter(prefix="/storage/metadata", tags=["metadata-storage"])


# ---------------------------------------------------------------------------
# Request / Response Models
# ---------------------------------------------------------------------------

class ProductUpsertRequest(BaseModel):
    item_id: str
    name: str
    brand: Optional[str] = None
    model_number: Optional[str] = None
    product_type: Optional[str] = None
    country: Optional[str] = None
    colors: list[str] = []
    styles: list[str] = []
    keywords: list[str] = []
    bullet_points: list[str] = []


class LinkImagesRequest(BaseModel):
    original_image_id: Optional[str] = None
    thumbnail_image_id: Optional[str] = None
    large_image_id: Optional[str] = None
    other_image_ids: list[str] = []


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@metadata_router.post("/product", response_model=dict, status_code=201)
def api_upsert_product(body: ProductUpsertRequest):
    """
    Create or update a product's metadata.
    If the item_id already exists, all non-image fields are overwritten.
    Image fields are managed separately via PATCH /product/{item_id}/images.
    """
    try:
        product = upsert_product(
            item_id=body.item_id,
            name=body.name,
            brand=body.brand,
            model_number=body.model_number,
            product_type=body.product_type,
            country=body.country,
            colors=body.colors,
            styles=body.styles,
            keywords=body.keywords,
            bullet_points=body.bullet_points,
        )
        return {"success": True, "product": product.as_dict()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@metadata_router.get("/product/{item_id}", response_model=dict)
def api_get_product(item_id: str):
    """Fetch a single product by its item_id."""
    try:
        product = get_product(item_id)
        if product is None:
            raise HTTPException(status_code=404, detail=f"Product '{item_id}' not found")
        return {"product": product.as_dict()}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@metadata_router.get("/products", response_model=dict)
def api_list_products(
    limit: int = 20,
    offset: int = 0,
    brand: Optional[str] = None,
    product_type: Optional[str] = None,
):
    """
    List products with optional filters.
    Query params: limit, offset, brand, product_type
    """
    try:
        products = list_products(
            limit=limit,
            offset=offset,
            brand=brand,
            product_type=product_type,
        )
        return {
            "count": len(products),
            "limit": limit,
            "offset": offset,
            "products": [p.as_dict() for p in products],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@metadata_router.delete("/product/{item_id}", response_model=dict)
def api_delete_product(item_id: str):
    """Delete a product and all its metadata by item_id."""
    try:
        deleted = delete_product(item_id)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Product '{item_id}' not found")
        return {"success": True, "deleted_item_id": item_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@metadata_router.patch("/product/{item_id}/images", response_model=dict)
def api_link_product_images(item_id: str, body: LinkImagesRequest):
    """
    Link MinIO image URLs to an existing product.
    Call this AFTER uploading images via POST /storage/image/product/upload.
    Only image fields are updated; all other fields are preserved.
    """
    try:
        product = link_product_images(
            item_id=item_id,
            original_image_id=body.original_image_id,
            thumbnail_image_id=body.thumbnail_image_id,
            large_image_id=body.large_image_id,
            other_image_ids=body.other_image_ids,
        )
        if product is None:
            raise HTTPException(status_code=404, detail=f"Product '{item_id}' not found")
        return {"success": True, "product": product.as_dict()}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
