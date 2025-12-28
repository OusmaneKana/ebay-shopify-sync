import json
from pymongo import MongoClient
from app.config import settings

client = MongoClient(settings.MONGO_URI)
db = client[settings.MONGO_DB]

normalized_collection = db["product_normalized"]

all_tags = set()

for doc in normalized_collection.find():
    if "tags" in doc:
        all_tags.update(doc["tags"])

# Export to JSON
with open("all_tags.json", "w") as f:
    json.dump(sorted(list(all_tags)), f, indent=2)

print(f"Exported {len(all_tags)} unique tags to all_tags.json")