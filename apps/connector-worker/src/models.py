from pydantic import BaseModel
from typing import List, Optional

class ProductDraft(BaseModel):
    id: str
    title: str
    brand: Optional[str] = None
    original_image_url: Optional[str] = None
    thumbnail_image_url: Optional[str] = None
    # Add other fields as needed, keeping it flexible
    categories: List[str] = []
    final_price: Optional[float] = None
    rating: Optional[float] = None
    availability: Optional[bool] = None
    metadata: dict = {}
