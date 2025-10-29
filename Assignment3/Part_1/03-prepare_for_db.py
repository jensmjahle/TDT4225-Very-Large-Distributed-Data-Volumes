# ------------------------------------------------------------
# Prepare merged, nested Movies dataset for MongoDB insertion
# ------------------------------------------------------------
import pandas as pd
import json
import os
from pathlib import Path

DATA_DIR = Path("../movies_clean")
OUT_FILE = Path("../movies_prepared/movies_prepared.json")

print("\n===== STEP 1: LOADING CLEANED DATA =====")

movies = pd.read_csv(DATA_DIR / "movies_clean.csv")
credits = pd.read_csv(DATA_DIR / "credits_clean.csv")
keywords = pd.read_csv(DATA_DIR / "keywords_clean.csv")
links = pd.read_csv(DATA_DIR / "links_clean.csv")
ratings = pd.read_csv(DATA_DIR / "ratings_clean.csv")

print(f"movies:  {movies.shape}")
print(f"credits: {credits.shape}")
print(f"keywords:{keywords.shape}")
print(f"links:   {links.shape}")
print(f"ratings: {ratings.shape}")

# ------------------------------------------------------------
# STEP 2: BUILD ONE DOCUMENT PER MOVIE
# ------------------------------------------------------------
print("\n===== STEP 2: BUILDING EMBEDDED MOVIE DOCUMENTS =====")

def safe_json(v):
    if pd.isna(v) or v == "":
        return []
    if isinstance(v, (list, dict)):
        return v
    try:
        return json.loads(v)
    except Exception:
        try:
            import ast
            return ast.literal_eval(v)
        except Exception:
            return []

docs = []
for _, m in movies.iterrows():
    mid = int(m["id"])
    doc = {
        "movie_id": mid,
        "title": m.get("title"),
        "overview": m.get("overview"),
        "budget": m.get("budget"),
        "revenue": m.get("revenue"),
        "runtime": m.get("runtime"),
        "release_date": m.get("release_date"),
        "year": int(m.get("year")) if not pd.isna(m.get("year")) else None,
        "vote_average": m.get("vote_average"),
        "vote_count": m.get("vote_count"),
        "popularity": m.get("popularity"),
        "genres": safe_json(m.get("genres")),
        "production_companies": safe_json(m.get("production_companies")),
        "production_countries": safe_json(m.get("production_countries")),
        "spoken_languages": safe_json(m.get("spoken_languages")),
        "belongs_to_collection": safe_json(m.get("belongs_to_collection")),
        "cast": [],
        "crew": [],
        "keywords": [],
        "links": {},
        "ratings": []
    }

    # attach credits
    subc = credits.loc[credits["movie_id"] == mid]
    if not subc.empty:
        doc["cast"] = safe_json(subc.iloc[0].get("cast"))
        doc["crew"] = safe_json(subc.iloc[0].get("crew"))

    # attach keywords
    subk = keywords.loc[keywords["movie_id"] == mid]
    if not subk.empty:
        doc["keywords"] = safe_json(subk.iloc[0].get("keywords"))

    # attach links
    subl = links.loc[links["movie_id"] == mid]
    if not subl.empty:
        doc["links"] = subl.iloc[0].to_dict()

    # attach ratings (many rows)
    subr = ratings.loc[ratings["movie_id"] == mid]
    if not subr.empty:
        doc["ratings"] = subr.to_dict(orient="records")

    docs.append(doc)

print(f"Built {len(docs)} movie documents with nested sub-structures.")

# ------------------------------------------------------------
# STEP 3: SAVE AS JSON (one line per document)
# ------------------------------------------------------------
os.makedirs(OUT_FILE.parent, exist_ok=True)
with open(OUT_FILE, "w", encoding="utf-8") as f:
    for d in docs:
        f.write(json.dumps(d, ensure_ascii=False) + "\n")

print(f"\nSaved merged dataset to: {OUT_FILE}")
print("=== PREPARATION COMPLETED SUCCESSFULLY ===")
