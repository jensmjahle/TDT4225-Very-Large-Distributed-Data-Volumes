# ------------------------------------------------------------
# Insert embedded Movies dataset into MongoDB
# ------------------------------------------------------------
import os
import json
from pathlib import Path
from DbConnector import DbConnector

DATA_FILE = Path("../movies_prepared/movies_prepared.json")
COLLECTION_NAME = "movies"

print("\n===== STEP 1: CONNECTING TO MONGODB =====")
connection = DbConnector()
db = connection.db

print("\n===== STEP 2: READING MERGED JSON =====")
records = []
with open(DATA_FILE, "r", encoding="utf-8") as f:
    for line in f:
        records.append(json.loads(line))
print(f"Loaded {len(records):,} documents from {DATA_FILE}")

print("\n===== STEP 3: CREATING COLLECTION =====")
if COLLECTION_NAME in db.list_collection_names():
    db[COLLECTION_NAME].drop()
    print(f"Dropped existing '{COLLECTION_NAME}' collection")
db.create_collection(COLLECTION_NAME)
print(f"Created collection '{COLLECTION_NAME}'")

print("\n===== STEP 4: INSERTING INTO MONGODB =====")
BATCH_SIZE = 1000
for i in range(0, len(records), BATCH_SIZE):
    batch = records[i:i+BATCH_SIZE]
    db[COLLECTION_NAME].insert_many(batch)
    print(f"Inserted {min(i+BATCH_SIZE, len(records))}/{len(records)}")

print("\n===== STEP 5: VERIFY COUNT =====")
print(f"{COLLECTION_NAME}: {db[COLLECTION_NAME].count_documents({}):,} documents")

connection.close_connection()
print("\n=== INSERTION COMPLETED SUCCESSFULLY ===")
