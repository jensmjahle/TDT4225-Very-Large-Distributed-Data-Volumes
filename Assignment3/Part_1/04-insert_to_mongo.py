# ------------------------------------------------------------
# Insert cleaned Movies dataset into MongoDB
# ------------------------------------------------------------
from DbConnector import DbConnector
import pandas as pd
import json
import os
import math


print("\n===== STEP 1: CONNECTING TO MONGODB =====")
connection = DbConnector()
db = connection.db

print("\n===== STEP 2: LOADING CLEANED CSV FILES =====")
DATA_DIR = "../movies_clean"

movies = pd.read_csv(os.path.join(DATA_DIR, "movies_clean.csv"))
credits = pd.read_csv(os.path.join(DATA_DIR, "credits_clean.csv"))
keywords = pd.read_csv(os.path.join(DATA_DIR, "keywords_clean.csv"))
links = pd.read_csv(os.path.join(DATA_DIR, "links_clean.csv"))
ratings = pd.read_csv(os.path.join(DATA_DIR, "ratings_clean.csv"))

print("Loaded:")
print(f"movies: {movies.shape}, credits: {credits.shape}, keywords: {keywords.shape}, links: {links.shape}, ratings: {ratings.shape}")

print("\n===== STEP 3: PREPARING COLLECTIONS =====")
collections = ["movies", "credits", "keywords", "links", "ratings"]
for col in collections:
    if col in db.list_collection_names():
        db[col].drop()
        print(f"Dropped existing collection '{col}'")
    db.create_collection(col)
    print(f"Created collection '{col}'")

print("\n===== STEP 4: INSERTING DATA (CHUNKED) =====")

def insert_collection(df, name, id_field=None, batch_size=5000):
    records = df.to_dict(orient="records")
    total = len(records)
    n_batches = math.ceil(total / batch_size)
    print(f"\nInserting {total:,} documents into '{name}' in {n_batches} batches...")
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        db[name].insert_many(batch)
        print(f"  → inserted {i + len(batch):,}/{total:,}")
    print(f"Finished inserting '{name}' collection.")

insert_collection(movies, "movies")
insert_collection(credits, "credits")
insert_collection(keywords, "keywords")
insert_collection(links, "links")
insert_collection(ratings, "ratings")

print("\n===== STEP 5: VERIFYING COUNTS =====")
for col in collections:
    print(f"{col}: {db[col].count_documents({}):,} documents")

print("\n===== STEP 6: CLOSING CONNECTION =====")
connection.close_connection()
print("MongoDB insertion completed successfully.")
