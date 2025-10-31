# ------------------------------------------------------------
# Prepare merged Movies dataset for MongoDB insertion
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

# ------------------------------------------------------------
# STEP 1.5: CLEAN AND ALIGN IDS (no filtering)
# ------------------------------------------------------------
print("\n===== STEP 1.5: CLEAN AND ALIGN IDS =====")

# Ensure consistent numeric types
links["tmdb_id"] = pd.to_numeric(links["tmdb_id"], errors="coerce").astype("Int64")
links["movie_lens_id"] = pd.to_numeric(links["movie_lens_id"], errors="coerce").astype("Int64")
ratings["movie_lens_id"] = pd.to_numeric(ratings["movie_lens_id"], errors="coerce").astype("Int64")

# Convert release_date to clean string for MongoDB
if "release_date" in movies.columns:
    movies["release_date"] = pd.to_datetime(movies["release_date"], errors="coerce")
    movies["release_date_str"] = movies["release_date"].dt.strftime("%Y-%m-%d")

# No filtering — keep all movies, credits, and keywords.
# Just ensure link mappings are valid and consistent.

# Drop rows in links with missing TMDb IDs
before_links = len(links)
links = links.dropna(subset=["tmdb_id"])
print(f"Links: removed {before_links - len(links)} rows with missing TMDb IDs")

# Drop invalid or missing ratings (if any)
before_ratings = len(ratings)
ratings = ratings.dropna(subset=["movie_lens_id"])
print(f"Ratings: removed {before_ratings - len(ratings)} rows with missing MovieLens IDs")

print(f"Movies retained: {len(movies)}")
print(f"Credits retained: {len(credits)}")
print(f"Keywords retained: {len(keywords)}")


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
    tmdb_id = int(m["id"])
    doc = {
        "_id": tmdb_id,
        "title": m.get("title"),
        "overview": m.get("overview"),
        "budget": m.get("budget"),
        "revenue": m.get("revenue"),
        "runtime": m.get("runtime"),
        "release_date": m.get("release_date_str"),
        "vote_average": m.get("vote_average"),
        "vote_count": m.get("vote_count"),
        "popularity": m.get("popularity"),
        "original_language": m.get("original_language"),
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

    # Attach credits
    subc = credits.loc[credits["movie_id"] == tmdb_id]
    if not subc.empty:
        doc["cast"] = safe_json(subc.iloc[0].get("cast"))
        doc["crew"] = safe_json(subc.iloc[0].get("crew"))

    # Attach keywords
    subk = keywords.loc[keywords["movie_id"] == tmdb_id]
    if not subk.empty:
        doc["keywords"] = safe_json(subk.iloc[0].get("keywords"))

    # Attach links (match on tmdb_id)
    subl = links.loc[links["tmdb_id"] == tmdb_id]
    if not subl.empty:
        doc["links"] = {
            "movie_lens_id": int(subl.iloc[0].get("movie_lens_id")),
            "imdb_id": subl.iloc[0].get("imdb_id"),
            "tmdb_id": int(subl.iloc[0].get("tmdb_id")),
        }

    docs.append(doc)

print(f"Built {len(docs)} movie documents.")

# ------------------------------------------------------------
# STEP 3: JOIN RATINGS WITH LINKS (add tmdb_id + imdb_id)
# ------------------------------------------------------------
print("\n===== STEP 3: JOINING RATINGS WITH LINKS =====")

ratings = ratings.merge(
    links[["movie_lens_id", "imdb_id", "tmdb_id"]],
    on="movie_lens_id",
    how="left"
)

# Convert timestamp in ratings to consistent string format
if "timestamp" in ratings.columns:
    ratings["timestamp"] = pd.to_datetime(ratings["timestamp"], errors="coerce")

    ratings["timestamp"] = ratings["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")


# Drop rows where we couldn't find a matching link
before = len(ratings)
ratings = ratings.dropna(subset=["tmdb_id"])
after = len(ratings)
print(f"Ratings joined with links: {before} -> {after}")

# ------------------------------------------------------------
# STEP 4: SAVE MOVIES JSON
# ------------------------------------------------------------
print("\n===== STEP 4: SAVING MOVIES JSON =====")
os.makedirs(OUT_MOVIES.parent, exist_ok=True)
with open(OUT_MOVIES, "w", encoding="utf-8") as f:
    for d in docs:
        f.write(json.dumps(d, ensure_ascii=False) + "\n")
print("Saved movie documents.")

# ------------------------------------------------------------
# STEP 5: SAVE RATINGS JSON
# ------------------------------------------------------------
print("\n===== STEP 5: SAVING RATINGS JSON =====")
ratings_docs = ratings.to_dict(orient="records")
with open(OUT_RATINGS, "w", encoding="utf-8") as f:
    for r in ratings_docs:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
print("Saved ratings documents.")

print(f"\nSaved movies to:  {OUT_MOVIES}")
print(f"Saved ratings to: {OUT_RATINGS}")
print("=== PREPARATION COMPLETED SUCCESSFULLY ===")