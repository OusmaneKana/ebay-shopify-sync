from pydantic import BaseModel
from typing import List, Optional

class EbayProduct(BaseModel):
    sku: str
    title: Optional[str]
    categoryId: Optional[str]
    images: List[str] = []
    quantity: int = 0
    price: Optional[str]
