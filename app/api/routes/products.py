from fastapi import APIRouter
from app.database.mongo import db

router = APIRouter()

@router.get("/")
async def list_products(limit: int = 50):
    docs = await db.product_normalized.find().limit(limit).to_list(None)
    return docs
