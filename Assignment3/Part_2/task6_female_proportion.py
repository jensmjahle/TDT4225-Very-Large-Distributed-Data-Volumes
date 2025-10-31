# ------------------------------------------------------------
# TASK 6: Female Cast Proportion by Decade (Top 5 Billed)
# ------------------------------------------------------------
from print_helper import print_table

class Task6:
    def __init__(self, db):
        self.db = db

    def run(self):
        print("\n--- TASK 6: Female Cast Proportion by Decade ---")

        pipeline = [
            # 1️⃣ Keep valid entries
            {"$match": {
                "year": {"$ne": None},
                "cast": {"$exists": True, "$ne": []}
            }},
            # 2️⃣ Compute decade
            {"$addFields": {
                "decade": {"$multiply": [{"$floor": {"$divide": ["$year", 10]}}, 10]}
            }},
            # 3️⃣ Keep only top 5 billed cast (order 0–4)
            {"$addFields": {
                "top5_cast": {
                    "$slice": [
                        {"$filter": {
                            "input": "$cast",
                            "as": "c",
                            "cond": {"$lte": ["$$c.order", 4]}
                        }},
                        5
                    ]
                }
            }},
            # 4️⃣ Compute per-movie female proportion
            {"$project": {
                "decade": 1,
                "female_count": {
                    "$size": {
                        "$filter": {
                            "input": "$top5_cast",
                            "as": "c",
                            "cond": {"$eq": ["$$c.gender", 1]}
                        }
                    }
                },
                "known_count": {
                    "$size": {
                        "$filter": {
                            "input": "$top5_cast",
                            "as": "c",
                            "cond": {"$in": ["$$c.gender", [1, 2]]}
                        }
                    }
                }
            }},
            {"$addFields": {
                "female_prop": {
                    "$cond": [
                        {"$gt": ["$known_count", 0]},
                        {"$divide": ["$female_count", "$known_count"]},
                        None
                    ]
                }
            }},
            # 5️⃣ Aggregate by decade
            {"$group": {
                "_id": "$decade",
                "avg_female_proportion": {"$avg": "$female_prop"},
                "movie_count": {"$sum": 1}
            }},
            # 6️⃣ Sort by female proportion desc
            {"$sort": {"avg_female_proportion": -1}}
        ]

        results = list(self.db.movies.aggregate(pipeline))

        for r in results:
            r["decade"] = r["_id"]
            del r["_id"]

        print_table(
            results,
            headers=["decade", "avg_female_proportion", "movie_count"]
        )
        print(f"\nReturned {len(results)} decade groups.")
