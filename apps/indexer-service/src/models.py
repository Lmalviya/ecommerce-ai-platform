from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

class ProductDraft(BaseModel):
    """
    The universal data structure for products in the system.
    Regardless of the source (Postgres, S3, CSV), all data is converted
    to this format before indexing.
    """
    id: str = Field(..., description="Unique identifier (Amazon Standard Identification Number)")
    title: str = Field(..., description="Product title")
    description: Optional[str] = Field(None, description="Detailed product description")
    
    # Pricing
    initial_price: Optional[float] = Field(None, description="Original price")
    final_price: Optional[float] = Field(None, description="Current/Discounted price")
    currency: str = Field("USD", description="Currency code")
    
    # Stats & Status
    availability: Optional[str] = Field("out of stock", description="Stock status")
    reviews_count: int = Field(0, description="Total number of reviews")
    rating: float = Field(0.0, description="Average customer rating (0-5)")
    
    # Classification & Identity
    categories: List[str] = Field(default_factory=list, description="Category breadcrumbs")
    brand: Optional[str] = Field(None, description="Manufacturer brand name")
    seller_name: Optional[str] = Field(None, description="Name of the seller")
    
    # Media & Links
    original_image_url: Optional[str] = Field(None, description="Primary image URL")
    thumbnail_image_url: Optional[str] = Field(None, description="Direct product link")
    
    # Specs
    features: List[str] = Field(default_factory=list, description="Bullet points/specifications")
    
    # Extra data (Source-specific)
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional source-specific fields")

    class Config:
        from_attributes = True # Allows compatibility with ORM objects (like from Postgres)