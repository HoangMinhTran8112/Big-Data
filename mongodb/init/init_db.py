# mongodb/init/init_db.py
import os
from pymongo import MongoClient, ASCENDING, DESCENDING

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017/?authSource=admin")
DB_NAME = os.getenv("MONGO_DB", "bdg")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

print(f"Connected to MongoDB, using database '{DB_NAME}'")

# Ensure collections exist
events = db["events"]
enriched = db["staging_enriched"]
scores = db["serve_scores"]

# Create indexes
events.create_index([("device_id", ASCENDING), ("ts", DESCENDING)], name="idx_device_ts")
events.create_index("status")

# Seed example doc
seed_doc = {
    "event_id": 0,
    "ts": "2025-09-01T00:00:00Z",
    "device_id": "seed",
    "reading": 42.0,
    "status": "ok"
}
events.insert_one(seed_doc)
print("Inserted seed event")

print("Indexes:", events.index_information())
