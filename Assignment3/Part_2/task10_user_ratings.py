from print_helper import print_table

class Task10:
    def __init__(self, db):
        self.db = db

    def run(self):
        print("\n--- TASK 10: User Rating Stats ---")

        pipeline = [
            # 1️⃣ Join ratings → movies to get genres
            {"$lookup": {
                "from": "movies",
                "localField": "movie_id",
                "foreignField": "_id",
                "as": "movie"
            }},
            {"$unwind": "$movie"},

            # 2️⃣ Extract each rating's genre (unwind movie.genres)
            {"$unwind": "$movie.genres"},
            {"$project": {
                "userId": 1,
                "rating": 1,
                "genre": "$movie.genres.name"
            }},

            # 3️⃣ Group by user to compute stats
            {"$group": {
                "_id": "$userId",
                "ratings_count": {"$sum": 1},
                "ratings": {"$push": "$rating"},
                "genres": {"$addToSet": "$genre"},
                "stddev": {"$stdDevPop": "$rating"}
            }},

            # 4️⃣ Compute variance and genre_count
            {"$addFields": {
                "variance": {"$pow": ["$stddev", 2]},
                "genre_count": {"$size": "$genres"}
            }},

            # 5️⃣ Keep only users with ≥20 ratings
            {"$match": {"ratings_count": {"$gte": 20}}},

            # 6️⃣ Clean projection
            {"$project": {
                "_id": 0,
                "user_id": "$_id",
                "ratings_count": 1,
                "genre_count": 1,
                "variance": {"$round": ["$variance", 3]}
            }}
        ]

        # Run aggregation
        base = list(self.db.ratings.aggregate(pipeline))

        # Split views
        top_genre = sorted(base, key=lambda x: x["genre_count"], reverse=True)[:10]
        top_var = sorted(base, key=lambda x: x["variance"], reverse=True)[:10]

        print("\n--- Top 10 Most Genre-Diverse Users ---")
        print_table(top_genre, headers=["user_id", "ratings_count", "genre_count", "variance"])

        print("\n--- Top 10 Highest Variance Users ---")
        print_table(top_var, headers=["user_id", "ratings_count", "genre_count", "variance"])

        print(f"\nProcessed {len(base)} users (with ≥20 ratings).")
