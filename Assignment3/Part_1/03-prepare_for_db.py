# ------------------------------------------------------------
# Prepare merged Movies dataset for MongoDB insertion
# (with ratings as a separate collection)
# ------------------------------------------------------------
import pandas as pd
import json
import os
from pathlib import Path

DATA_DIR = Path("../movies_clean")
OUT_MOVIES = Path("../movies_prepared/movies_prepared.json")
OUT_RATINGS = Path("../movies_prepared/ratings_prepared.json")

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

# --- STEP 1.5: FILTER TO SMALL SUBSET (present in links_small/ratings_small) ---
ids_links   = set(links["movie_id"].dropna().astype("Int64"))
ids_ratings = set(ratings["movie_id"].dropna().astype("Int64"))

# keep any movie that appears in links or ratings (union)
keep_ids = ids_links | ids_ratings

bm = len(movies)
bc = len(credits)
bk = len(keywords)

movies   = movies[movies["id"].isin(keep_ids)].copy()
credits  = credits[credits["movie_id"].isin(keep_ids)].copy()
keywords = keywords[keywords["movie_id"].isin(keep_ids)].copy()

print(f"Filtered movies by links/ratings: {bm} -> {len(movies)}")
print(f"Filtered credits by links/ratings: {bc} -> {len(credits)}")
print(f"Filtered keywords by links/ratings: {bk} -> {len(keywords)}")



# ------------------------------------------------------------
# STEP 2: BUILD ONE DOCUMENT PER MOVIE (no ratings)
# ------------------------------------------------------------
print("\n===== STEP 2: BUILDING MOVIE DOCUMENTS =====")

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
        "_id": mid,
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
        "links": {}
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

    docs.append(doc)

print(f"Built {len(docs)} movie documents.")

# ------------------------------------------------------------
# STEP 3: SAVE MOVIES JSON
# ------------------------------------------------------------
print("\n===== STEP 3: SAVING MOVIES JSON =====")
os.makedirs(OUT_MOVIES.parent, exist_ok=True)
with open(OUT_MOVIES, "w", encoding="utf-8") as f:
    for d in docs:
        f.write(json.dumps(d, ensure_ascii=False) + "\n")
print("Saved movie documents.")

# ------------------------------------------------------------
# STEP 4: SAVE RATINGS JSON
# ------------------------------------------------------------
print("\n===== STEP 4: SAVING RATINGS JSON =====")
ratings_docs = ratings.to_dict(orient="records")
with open(OUT_RATINGS, "w", encoding="utf-8") as f:
    for r in ratings_docs:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
print("Saved ratings documents.")

print(f"\nSaved movies to:  {OUT_MOVIES}")
print(f"Saved ratings to: {OUT_RATINGS}")
print("=== PREPARATION COMPLETED SUCCESSFULLY ===")
