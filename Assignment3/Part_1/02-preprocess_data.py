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
# STEP 2: REMOVE DUPLICATES AND FIX BROKEN ROWS
# ------------------------------------------------------------
print("\n===== STEP 2: REMOVE DUPLICATES AND FIX BROKEN ROWS =====")


def drop_duplicates_keep_latest(df, keys, name):
    before = len(df)

    # Drop exact (fully identical) duplicates first
    df = df.drop_duplicates(keep="last")

    # Then drop duplicates based on the key columns (keep last = most recent)
    df = df.drop_duplicates(subset=keys, keep="last")

    removed = before - len(df)
    print(f"{name}: removed {removed} duplicates (kept most recent per {keys})")
    return df


movies = drop_duplicates_keep_latest(movies, ["id"], "Movies Metadata")
credits = drop_duplicates_keep_latest(credits, ["id"], "Credits")
keywords = drop_duplicates_keep_latest(keywords, ["id"], "Keywords")
links = drop_duplicates_keep_latest(links, ["movieId"], "Links")
ratings = drop_duplicates_keep_latest(ratings, ["movieId", "userId"], "Ratings")



def fix_broken_rows(df):
    valid_adults = {"True", "False"}
    broken_idx = df[~df["adult"].isin(valid_adults)].index

    repaired_rows = []
    for idx in broken_idx:
        if idx - 1 in df.index:
            prev_row = df.loc[idx - 1].copy()
            curr_row = df.loc[idx].copy()

            prev_row["overview"] = (
                str(prev_row.get("overview", "")).strip() + " " + str(curr_row["adult"]).strip()
            ).strip()

            for col in df.columns:
                if pd.isna(prev_row[col]) or prev_row[col] == "":
                    prev_row[col] = curr_row[col]

            repaired_rows.append((idx - 1, prev_row))

    for idx, new_row in repaired_rows:
        df.loc[idx] = new_row

    df = df.drop(broken_idx).reset_index(drop=True)
    print(f"Fixed {len(broken_idx)} broken lines (merged into previous rows)")
    return df


movies = fix_broken_rows(movies)


# ----------------------------
# STEP 3: NORMALIZE IDS (fixed)
# ----------------------------
print("\n===== STEP 3: NORMALIZE IDS (FIXED) =====")

# 1) movies: convert id to numeric (tmdb id) and keep imdb_id as-is (string like "tt0123456")
if "id" in movies.columns:
    movies["id"] = pd.to_numeric(movies["id"], errors="coerce").astype("Int64")
    invalid = movies["id"].isna().sum()
    print(f"movies: converted 'id' to numeric ({invalid} invalid IDs)")

# ensure movies.imdb_id is string and trimmed
if "imdb_id" in movies.columns:
    movies["imdb_id"] = movies["imdb_id"].astype("string").str.strip()

# 2) credits: id in credits is a TMDb movie id -> rename to movie_id (tmdb)
if "id" in credits.columns and "movie_id" not in credits.columns:
    credits.rename(columns={"id": "movie_id"}, inplace=True)
credits["movie_id"] = pd.to_numeric(credits["movie_id"], errors="coerce").astype("Int64")
invalid = credits["movie_id"].isna().sum()
print(f"credits: normalized 'movie_id' (tmdb) ({invalid} invalid IDs)")

# 3) keywords: id is TMDb movie id -> rename and normalize
if "id" in keywords.columns and "movie_id" not in keywords.columns:
    keywords.rename(columns={"id": "movie_id"}, inplace=True)
keywords["movie_id"] = pd.to_numeric(keywords["movie_id"], errors="coerce").astype("Int64")
invalid = keywords["movie_id"].isna().sum()
print(f"keywords: normalized 'movie_id' (tmdb) ({invalid} invalid IDs)")

# 4) links: this file maps MovieLens movieId -> tmdbId / imdbId
#    - rename movieId -> movie_lens_id (keep as integer)
#    - create tmdb_id as Int64 from tmdbId (floats like 10858.0)
#    - create imdb_id as the same format as movies.imdb_id (e.g. 'tt0113002')
if "movieId" in links.columns:
    links.rename(columns={"movieId": "movie_lens_id"}, inplace=True)
links["movie_lens_id"] = pd.to_numeric(links["movie_lens_id"], errors="coerce").astype("Int64")

# tmdbId -> tmdb_id (Int)
if "tmdbId" in links.columns:
    links["tmdb_id"] = pd.to_numeric(links["tmdbId"], errors="coerce").astype("Int64")
else:
    links["tmdb_id"] = pd.Series([pd.NA]*len(links), dtype="Int64")

# imdbId -> imdb_id formatted like movies['imdb_id'] ("tt" + zero-padded 7-digit numeric)
def format_imdb(val):
    # links may have floats/ints or NaN; some imdbId are missing
    try:
        if pd.isna(val):
            return pd.NA
        v = int(float(val))
        # pad to 7 digits (matches 'tt0000001' style)
        return f"tt{v:07d}"
    except Exception:
        return pd.NA

if "imdbId" in links.columns:
    links["imdb_id"] = links["imdbId"].apply(format_imdb).astype("string")
else:
    links["imdb_id"] = pd.Series([pd.NA]*len(links), dtype="string")

# report links normalization
print(f"links: normalized movie_lens_id (count={links['movie_lens_id'].notna().sum()})")
print(f"links: normalized tmdb_id (count={links['tmdb_id'].notna().sum()})")
print(f"links: normalized imdb_id (count={links['imdb_id'].notna().sum()})")

# 5) ratings: rename movieId -> movie_lens_id (so it matches links)
if "movieId" in ratings.columns:
    ratings.rename(columns={"movieId": "movie_lens_id"}, inplace=True)
ratings["movie_lens_id"] = pd.to_numeric(ratings["movie_lens_id"], errors="coerce").astype("Int64")
invalid = ratings["movie_lens_id"].isna().sum()
print(f"ratings: normalized 'movie_lens_id' ({invalid} invalid IDs)")


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

# Convert release_date to datetime64[ns]
if "release_date" in movies.columns:
    movies["release_date"] = pd.to_datetime(movies["release_date"], errors="coerce")
    invalid = movies["release_date"].isna().sum()
    print(f"movies: converted 'release_date' to datetime64 ({invalid} invalid dates)")

# Convert timestamp (UNIX) to datetime64[ns]
if "timestamp" in ratings.columns:
    ratings["timestamp"] = pd.to_datetime(ratings["timestamp"], unit="s", errors="coerce")
    invalid = ratings["timestamp"].isna().sum()
    print(f"ratings: converted 'timestamp' from UNIX to datetime64 ({invalid} invalid timestamps)")


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
# STEP 6: FILTER OUT ROWS WITHOUT VALID IDS (fixed for links/ratings)
# ------------------------------------------------------------
print("\n===== STEP 6: FILTER OUT ROWS WITHOUT VALID IDS =====")

def drop_empty_ids(df, id_col, name):
    if id_col not in df.columns:
        print(f"{name}: no column '{id_col}', skipped")
        return df
    before = len(df)
    df = df[df[id_col].notna()]
    removed = before - len(df)
    print(f"{name}: removed {removed} rows without valid {id_col}")
    return df

movies = drop_empty_ids(movies, "id", "movies")
credits = drop_empty_ids(credits, "movie_id", "credits")
keywords = drop_empty_ids(keywords, "movie_id", "keywords")
links = drop_empty_ids(links, "movie_lens_id", "links")
ratings = drop_empty_ids(ratings, "movie_lens_id", "ratings")

# ------------------------------------------------------------
# STEP 7: VALIDATE FOREIGN KEYS (corrected)
# ------------------------------------------------------------
# We'll:
# - Validate credits/keywords using tmdb ids (movies.id)
# - Evaluate links matching to movies via tmdb_id and imdb_id
# - Validate ratings using links.movie_lens_id (not movies.id)
print("\n===== STEP 7: VALIDATE FOREIGN KEYS (CORRECTED) =====")

# prepare sets
valid_tmdb_ids = set(movies["id"].dropna().astype("Int64"))
valid_imdb_ids = set(movies["imdb_id"].dropna().astype("string"))

def count_invalid_tmdb(df, col, name):
    if col not in df.columns:
        print(f"{name}: no column '{col}', skipped")
        return
    total = len(df)
    invalid = (~df[col].isin(valid_tmdb_ids)).sum()
    print(f"{name}: {invalid} rows where {col} is not present in movies.id (out of {total})")

def count_invalid_imdb(df, col, name):
    if col not in df.columns:
        print(f"{name}: no column '{col}', skipped")
        return
    total = len(df)
    invalid = (~df[col].isin(valid_imdb_ids)).sum()
    print(f"{name}: {invalid} rows where {col} is not present in movies.imdb_id (out of {total})")

# credits and keywords reference TMDb movie id (movie_id)
count_invalid_tmdb(credits, "movie_id", "credits")
count_invalid_tmdb(keywords, "movie_id", "keywords")

# links: check two ways to link to movies:
# - tmdb_id (directly to movies.id)
# - imdb_id (after formatting) to movies.imdb_id
# Count rows that match neither
links_total = len(links)
links_match_tmdb = links["tmdb_id"].isin(valid_tmdb_ids)
links_match_imdb = links["imdb_id"].isin(valid_imdb_ids)
links_matches = links_match_tmdb | links_match_imdb
links_unmatched = (~links_matches).sum()
print(f"links: total rows {links_total}; matched by tmdb_id: {links_match_tmdb.sum()}; matched by imdb_id: {links_match_imdb.sum()}; unmatched (by neither): {links_unmatched}")

# ratings: validate MovieLens ids against links.movie_lens_id
valid_movie_lens_ids = set(links["movie_lens_id"].dropna().astype("Int64"))
if "movie_lens_id" in ratings.columns:
    total = len(ratings)
    invalid = (~ratings["movie_lens_id"].isin(valid_movie_lens_ids)).sum()
    print(f"ratings: {invalid} rows where movie_lens_id is not present in links.movie_lens_id (out of {total})")
else:
    print("ratings: no column 'movie_lens_id', skipped")

print("Foreign key validation completed.")



# ------------------------------------------------------------
# STEP 8: SAVE CLEAN FILES
# ------------------------------------------------------------
print("\n===== STEP 8: SAVE CLEAN FILES =====")

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