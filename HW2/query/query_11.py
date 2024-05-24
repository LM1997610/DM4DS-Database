
import time
import pandas as pd

from tabulate import tabulate
from pymongo import MongoClient


client = MongoClient('mongodb://localhost:27017/')
db = client['soccer_db']

pipeline = [

    {"$lookup": {"from": "matches",
                 "localField": "id",
                 "foreignField": "league_id",
                 "as": "all_matches"}   },

    {"$unwind": "$all_matches"},

    {"$addFields": {"total_goals": { "$add": ["$all_matches.home_team_goal", "$all_matches.away_team_goal"]}}   },

    {"$group": {"_id": {"league_id": "$id", "season": "$all_matches.season"},
                "total_goals": {"$sum": "$total_goals"},
                "match_count": {"$sum": 1},
                "league_name": {"$first": "$name"}} },

    {"$addFields": {"avg_total_goals": {"$divide": ["$total_goals", "$match_count"]}}   },

    {"$sort": {"avg_total_goals": -1}  },

    {"$limit": 3},
    {"$project": {"avg_total_goals": 1,
                  "_id": 0,
                  "league_name": 1,
                  "season": "$_id.season",
                  "league_id": "$_id.league_id"}  }   
                  
]


start_time = time.time()   

result = list(db.leagues.aggregate(pipeline))

df = pd.DataFrame.from_records(result) 

print()
print(tabulate(df, headers='keys', tablefmt="psql", numalign='center', stralign="center"))

print(f"\n Execution time: {time.time() - start_time:.3f} seconds")
