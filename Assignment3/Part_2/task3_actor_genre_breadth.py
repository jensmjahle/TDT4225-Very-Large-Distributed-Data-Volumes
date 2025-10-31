# ------------------------------------------------------------
# TASK 3: Top 10 Actors with Widest Genre Breadth
# ------------------------------------------------------------
from print_helper import print_table

class Task3:
    def __init__(self, db):
        self.db = db

    def run(self):
        print("\n--- TASK 3: Actors with Widest Genre Breadth ---")

        pipeline = [
            #  Only keep movies that have both cast and genres
            {"$match": {"cast": {"$exists": True, "$ne": []},
                        "genres": {"$exists": True, "$ne": []}}},
            #  Unwind cast and genres
            {"$unwind": "$cast"},
            {"$unwind": "$genres"},
            #  Group by actor name
            {"$group": {
                "_id": "$cast.name",
                "movie_ids": {"$addToSet": "$_id"},
                "genres": {"$addToSet": "$genres.name"}
            }},
            #  Compute counts and slice example genres
            {"$project": {
                "actor": "$_id",
                "_id": 0,
                "movie_count": {"$size": "$movie_ids"},
                "genre_count": {"$size": "$genres"},
                "example_genres": {"$slice": ["$genres", 5]}
            }},
            #  Only actors with ≥10 credited movies
            {"$match": {"movie_count": {"$gte": 10}}},
            #  Sort by genre_count, then movie_count, then name
            {"$sort": {"genre_count": -1, "movie_count": -1, "actor": 1}},
            {"$limit": 10}
        ]

        results = list(self.db.movies.aggregate(pipeline))

        print_table(results,
                    headers=["actor", "movie_count", "genre_count", "example_genres"])
        print(f"\nReturned {len(results)} actors.")
