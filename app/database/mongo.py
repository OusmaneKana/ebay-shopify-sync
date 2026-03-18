from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings

client = AsyncIOMotorClient(settings.MONGO_URI)
db = client[settings.MONGO_DB]


def close_mongo_client() -> None:
	"""Close Mongo client to stop background monitor threads."""
	try:
		client.close()
	except Exception:
		# Best-effort shutdown; avoid masking the real shutdown reason.
		pass
