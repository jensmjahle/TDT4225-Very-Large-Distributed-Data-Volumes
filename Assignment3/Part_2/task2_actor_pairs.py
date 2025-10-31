# ------------------------------------------------------------
# TASK 2: Actor pairs with ≥3 co-appearances (optimized)
# ------------------------------------------------------------
from print_helper import print_table

class Task2:
    def __init__(self, db):
        self.db = db

    def run(self):
        print("\n--- TASK 2: Actor Pairs with ≥3 Co-Appearances ---")

        pipeline = [
            #  Only keep movies with valid cast and vote_average
            {"$match": {"cast": {"$exists": True, "$ne": []}}},
            # Generate all unique actor pairs inside each movie
            {
                "$project": {
                    "vote_average": 1,
                    "pairs": {
                        "$function": {
                            "body": """
                                function(cast) {
                                    if (!Array.isArray(cast)) return [];
                                    const names = cast.map(c => c.name).filter(Boolean);
                                    const pairs = [];
                                    for (let i = 0; i < names.length; i++) {
                                        for (let j = i + 1; j < names.length; j++) {
                                            const a = names[i];
                                            const b = names[j];
                                            if (a && b) pairs.push([a, b]);
                                        }
                                    }
                                    return pairs;
                                }
                            """,
                            "args": ["$cast"],
                            "lang": "js",
                        }
                    },
                }
            },
            #  Unwind the generated actor pairs
            {"$unwind": "$pairs"},
            #  Group by pair and compute stats
            {
                "$group": {
                    "_id": "$pairs",
                    "co_appearances": {"$sum": 1},
                    "avg_vote": {"$avg": "$vote_average"},
                }
            },
            #  Keep only pairs with ≥3 shared movies
            {"$match": {"co_appearances": {"$gte": 3}}},
            # Sort
            {"$sort": {"co_appearances": -1, "_id": 1}},
            {"$limit": 20},
        ]

        results = list(self.db.movies.aggregate(pipeline))

        for r in results:
            r["actor1"], r["actor2"] = r["_id"]
            del r["_id"]

        print_table(results, headers=["actor1", "actor2", "co_appearances", "avg_vote"])
        print(f"\nReturned {len(results)} actor pairs.")
