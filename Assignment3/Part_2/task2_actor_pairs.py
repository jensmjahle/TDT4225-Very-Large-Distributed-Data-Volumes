# ------------------------------------------------------------
# TASK 2: Actor pairs with ≥3 co-appearances
# ------------------------------------------------------------
from print_helper import print_table

class Task2:
    def __init__(self, db):
        self.db = db

    def run(self):
        print("\n--- TASK 2: Actor Pairs with ≥3 Co-Appearances ---")

        pipeline = [
            # 1️⃣ unwind cast
            {"$unwind": "$cast"},
            {"$project": {
                "movie_id": 1,
                "vote_average": 1,
                "actor": "$cast.name"
            }},
            # 2️⃣ self join on same movie to form all actor pairs (A,B)
            {"$lookup": {
                "from": "movies",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "movie"
            }},
            {"$unwind": "$movie"},
            {"$unwind": "$movie.cast"},
            {"$project": {
                "actor1": "$actor",
                "actor2": "$movie.cast.name",
                "vote_average": "$vote_average"
            }},
            # 3️⃣ remove self-pairs (A==B)
            {"$match": {"$expr": {"$ne": ["$actor1", "$actor2"]}}},
            # 4️⃣ normalize pair order so (A,B) == (B,A)
            {"$addFields": {
                "pair": {
                    "$cond": [
                        {"$lt": ["$actor1", "$actor2"]},
                        ["$actor1", "$actor2"],
                        ["$actor2", "$actor1"]
                    ]
                }
            }},
            # 5️⃣ group by normalized pair
            {"$group": {
                "_id": "$pair",
                "co_appearances": {"$sum": 1},
                "avg_vote": {"$avg": "$vote_average"}
            }},
            # 6️⃣ keep only pairs appearing in ≥3 movies
            {"$match": {"co_appearances": {"$gte": 3}}},
            # 7️⃣ sort descending by number of co-appearances
            {"$sort": {"co_appearances": -1, "_id": 1}},
            {"$limit": 20}
        ]

        results = list(self.db.movies.aggregate(pipeline))
        for r in results:
            r["actor1"], r["actor2"] = r["_id"]
            del r["_id"]

        print_table(results, headers=["actor1", "actor2", "co_appearances", "avg_vote"])
        print(f"\nReturned {len(results)} actor pairs.")
