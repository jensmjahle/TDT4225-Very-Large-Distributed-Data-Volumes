# ------------------------------------------------------------
# Data Cleaning & Preprocessing for The Movies Dataset

import pandas as pd
import numpy as np
import json
import ast
import os

MOVIES_PATH = "../movies/movies_metadata.csv"
CREDITS_PATH = "../movies/credits.csv"
KEYWORDS_PATH = "../movies/keywords.csv"
LINKS_PATH = "../movies/links.csv"
RATINGS_PATH = "../movies/ratings.csv"

OUT_MOVIES = "../movies_clean/movies_clean.csv"
OUT_CREDITS = "../movies_clean/credits_clean.csv"
OUT_KEYWORDS = "../movies_clean/keywords_clean.csv"
OUT_LINKS = "../movies_clean/links_clean.csv"
OUT_RATINGS = "../movies_clean/ratings_clean.csv"

print("\n===== STEP 1: LOADING RAW DATA =====")

bad_movie_lines = []
bad_credit_lines = []
bad_keyword_lines = []
bad_links_lines = []
bad_ratings_lines = []

def log_bad_movie(line): bad_movie_lines.append(line); return None
def log_bad_credit(line): bad_credit_lines.append(line); return None
def log_bad_keyword(line): bad_keyword_lines.append(line); return None
def log_bad_links(line): bad_links_lines.append(line); return None
def log_bad_ratings(line): bad_ratings_lines.append(line); return None

movies = pd.read_csv(MOVIES_PATH, engine="python", sep=",", quotechar='"', on_bad_lines=lambda l: log_bad_movie(l))
credits = pd.read_csv(CREDITS_PATH, engine="python", sep=",", quotechar='"', on_bad_lines=lambda l: log_bad_credit(l))
keywords = pd.read_csv(KEYWORDS_PATH, engine="python", sep=",", quotechar='"', on_bad_lines=lambda l: log_bad_keyword(l))
links = pd.read_csv(LINKS_PATH, engine="python", sep=",", quotechar='"', on_bad_lines=lambda l: log_bad_links(l))
ratings = pd.read_csv(RATINGS_PATH, engine="python", sep=",", quotechar='"', on_bad_lines=lambda l: log_bad_ratings(l))

print(f"Loaded movies:  {movies.shape}")
print(f"Loaded credits: {credits.shape}")
print(f"Loaded keywords:{keywords.shape}")
print(f"Loaded links:   {links.shape}")
print(f"Loaded ratings: {ratings.shape}")

# ------------------------------------------------------------
# STEP 2: REMOVE FULL DUPLICATES (every column is identical)
# ------------------------------------------------------------
print("\n===== STEP 2: REMOVE FULL DUPLICATES =====")

before = len(movies)
movies = movies.drop_duplicates(keep="first")
print(f"movies: removed {before - len(movies)} duplicates")

before = len(credits)
credits = credits.drop_duplicates(keep="first")
print(f"credits: removed {before - len(credits)} duplicates")

before = len(keywords)
keywords = keywords.drop_duplicates(keep="first")
print(f"keywords: removed {before - len(keywords)} duplicates")

before = len(links)
links = links.drop_duplicates(keep="first")
print(f"links: removed {before - len(links)} duplicates")

before = len(ratings)
ratings = ratings.drop_duplicates(keep="first")
print(f"ratings: removed {before - len(ratings)} duplicates")


# ------------------------------------------------------------
# STEP 3: NORMALIZE IDS
# ------------------------------------------------------------
# Ensures all datasets are using a consistent numeric movie ID.
# - Converts string IDs to numeric while invalid values become NaN
# - Rename id to movie_id for clarity
print("\n===== STEP 3: NORMALIZE IDS =====")

if "id" in movies.columns:
    movies["id"] = pd.to_numeric(movies["id"], errors="coerce").astype("Int64")
    invalid = movies["id"].isna().sum()
    print(f"movies: converted 'id' to numeric ({invalid} invalid IDs)")

if "id" in credits.columns and "movie_id" not in credits.columns:
    credits.rename(columns={"id": "movie_id"}, inplace=True)
credits["movie_id"] = pd.to_numeric(credits["movie_id"], errors="coerce").astype("Int64")
invalid = credits["movie_id"].isna().sum()
print(f"credits: normalized 'movie_id' ({invalid} invalid IDs)")


if "id" in keywords.columns and "movie_id" not in keywords.columns:
    keywords.rename(columns={"id": "movie_id"}, inplace=True)
keywords["movie_id"] = pd.to_numeric(keywords["movie_id"], errors="coerce").astype("Int64")
invalid = keywords["movie_id"].isna().sum()
print(f"keywords: normalized 'movie_id' ({invalid} invalid IDs)")

if "movieId" in links.columns:
    links.rename(columns={"movieId": "movie_id"}, inplace=True)
links["movie_id"] = pd.to_numeric(links["movie_id"], errors="coerce").astype("Int64")
invalid = links["movie_id"].isna().sum()
print(f"links: normalized 'movie_id' ({invalid} invalid IDs)")

if "movieId" in ratings.columns:
    ratings.rename(columns={"movieId": "movie_id"}, inplace=True)
ratings["movie_id"] = pd.to_numeric(ratings["movie_id"], errors="coerce").astype("Int64")
invalid = ratings["movie_id"].isna().sum()
print(f"ratings: normalized 'movie_id' ({invalid} invalid IDs)")


# ------------------------------------------------------------
# Remove corrupted data adult not true/false

# ------------------------------------------------------------
# STEP 4: CONVERT NUMERIC + DATE TYPES
# ------------------------------------------------------------
print("\n===== STEP 4: TYPE CONVERSION =====")

numeric_cols = ["budget", "revenue", "runtime", "vote_average", "vote_count", "popularity"]
for col in numeric_cols:
    if col in movies.columns:
        movies[col] = pd.to_numeric(movies[col], errors="coerce")

if "release_date" in movies.columns:
    movies["release_date"] = pd.to_datetime(movies["release_date"], errors="coerce")
    movies["year"] = movies["release_date"].dt.year.astype("Int64")


# ------------------------------------------------------------
# STEP 5: PARSE JSON-LIKE FIELDS
# ------------------------------------------------------------
print("\n===== STEP 5: PARSE JSON-LIKE COLUMNS =====")

def parse_json_safe(v):
    if pd.isna(v) or v == "":
        return []
    if isinstance(v, (list, dict)):
        return v
    try:
        return json.loads(v)
    except Exception:
        try:
            return ast.literal_eval(v)
        except Exception:
            return []

for col in ["genres", "production_companies", "production_countries", "spoken_languages", "belongs_to_collection"]:
    if col in movies.columns:
        movies[col] = movies[col].apply(parse_json_safe)

for col in ["cast", "crew"]:
    if col in credits.columns:
        credits[col] = credits[col].apply(parse_json_safe)

if "keywords" in keywords.columns:
    keywords["keywords"] = keywords["keywords"].apply(parse_json_safe)

# ------------------------------------------------------------
# STEP 6: DROP EMPTY IDS
# ------------------------------------------------------------
print("\n===== STEP 6: FILTER OUT ROWS WITHOUT VALID IDS =====")

def drop_empty_ids(df, id_col, name):
    before = len(df)
    df = df[df[id_col].notna()]
    removed = before - len(df)
    print(f"{name}: removed {removed} rows without valid {id_col}")
    return df

movies = drop_empty_ids(movies, "id", "movies")
credits = drop_empty_ids(credits, "movie_id", "credits")
keywords = drop_empty_ids(keywords, "movie_id", "keywords")
links = drop_empty_ids(links, "movie_id", "links")
ratings = drop_empty_ids(ratings, "movie_id", "ratings")


# ------------------------------------------------------------
# STEP 7: VALIDATE FOREIGN KEYS (movie_id references)
# ------------------------------------------------------------
# Ensures every record in the secondary datasets refers to an existing movie.
# Any row whose movie_id is not found in movies['id'] is dropped.

print("\n===== STEP 7: VALIDATE FOREIGN KEYS =====")

valid_ids = set(movies["id"].dropna().astype("Int64"))

def validate_reference(df, name):
    if "movie_id" not in df.columns:
        print(f"{name}: no movie_id column, skipped validation")
        return df
    before = len(df)
    df = df[df["movie_id"].isin(valid_ids)]
    removed = before - len(df)
    print(f"{name}: removed {removed} rows with invalid movie_id references")
    return df

credits = validate_reference(credits, "credits")
keywords = validate_reference(keywords, "keywords")
links = validate_reference(links, "links")
ratings = validate_reference(ratings, "ratings")

print("Foreign key validation completed.")


# ------------------------------------------------------------
# STEP 7: SAVE CLEAN FILES
# ------------------------------------------------------------
print("\n===== STEP 7: SAVE CLEAN FILES =====")

os.makedirs(os.path.dirname(OUT_MOVIES), exist_ok=True)

movies.to_csv(OUT_MOVIES, index=False)
credits.to_csv(OUT_CREDITS, index=False)
keywords.to_csv(OUT_KEYWORDS, index=False)
links.to_csv(OUT_LINKS, index=False)
ratings.to_csv(OUT_RATINGS, index=False)

print("Cleaned files saved:")
print(f"movies:  {OUT_MOVIES} ({movies.shape})")
print(f"credits: {OUT_CREDITS} ({credits.shape})")
print(f"keywords:{OUT_KEYWORDS} ({keywords.shape})")
print(f"links:   {OUT_LINKS} ({links.shape})")
print(f"ratings: {OUT_RATINGS} ({ratings.shape})")

print("\n=== PREPROCESSING COMPLETED SUCCESSFULLY ===")
