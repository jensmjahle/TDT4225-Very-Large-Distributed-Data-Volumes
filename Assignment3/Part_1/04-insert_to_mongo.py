# ------------------------------------------------------------
# Insert Movies and Ratings datasets into MongoDB
# ------------------------------------------------------------
import json
from pathlib import Path
from DbConnector import DbConnector

DATA_DIR = Path("../movies_prepared")
MOVIES_FILE = DATA_DIR / "movies_prepared.json"
RATINGS_FILE = DATA_DIR / "ratings_prepared.json"

print("\n===== STEP 1: CONNECTING TO MONGODB =====")
connection = DbConnector()
db = connection.db

# ------------------------------------------------------------
# Insert Movies
# ------------------------------------------------------------
print("\n===== STEP 2: INSERTING MOVIES =====")
movies = []
with open(MOVIES_FILE, "r", encoding="utf-8") as f:
    for line in f:
        movies.append(json.loads(line))
print(f"Loaded {len(movies):,} movie documents")

if "movies" in db.list_collection_names():
    db["movies"].drop()
    print("Dropped existing 'movies' collection")
db.create_collection("movies")
db["movies"].insert_many(movies)
print(f"Inserted {len(movies):,} movies")

print("Creating text index on 'overview' and 'tagline' ...")
db["movies"].create_index([
    ("overview", "text"),
    ("tagline", "text"),
    ("keywords.name", "text")
])


# ------------------------------------------------------------
# Insert Ratings
# ------------------------------------------------------------
print("\n===== STEP 3: INSERTING RATINGS =====")
ratings = []
with open(RATINGS_FILE, "r", encoding="utf-8") as f:
    for line in f:
        ratings.append(json.loads(line))
print(f"Loaded {len(ratings):,} ratings")

if "ratings" in db.list_collection_names():
    db["ratings"].drop()
    print("Dropped existing 'ratings' collection")
db.create_collection("ratings")
BATCH_SIZE = 10000
for i in range(0, len(ratings), BATCH_SIZE):
    batch = ratings[i:i + BATCH_SIZE]
    db["ratings"].insert_many(batch)
    print(f"Inserted {min(i + BATCH_SIZE, len(ratings))}/{len(ratings)}")

print("\n===== STEP 4: VERIFYING COUNTS =====")
print(f"movies:  {db['movies'].count_documents({}):,}")
print(f"ratings: {db['ratings'].count_documents({}):,}")

connection.close_connection()
print("\n=== INSERTION COMPLETED SUCCESSFULLY ===")
