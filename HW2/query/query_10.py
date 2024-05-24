
import time
import pandas as pd

from tabulate import tabulate
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['soccer_db']

pipeline = [

    {"$addFields": {"b365h_num": {"$toDouble": {"$ifNull": ["$b365h", 0]}},
                    "b365a_num": {"$toDouble": {"$ifNull": ["$b365a", 0]}}   }},

    {"$addFields": {"odds_diff": {"$abs": {"$subtract": ["$b365h_num", "$b365a_num"]}}  }},

    {"$sort": {"odds_diff": -1}},

    {"$limit": 1},

    {"$lookup": {"from": "leagues",
                 "localField": "league_id",
                 "foreignField": "id",
                 "as": "league_info" }},

    {"$unwind": "$league_info"},

    {"$lookup": {"from": "teams",
                 "localField": "home_team_api_id",
                 "foreignField": "team_api_id",
                 "as": "home_team_info"     }},

    {"$unwind": "$home_team_info"},
    {"$lookup": {"from": "teams",
                 "localField": "away_team_api_id",
                 "foreignField": "team_api_id",
                 "as": "away_team_info"     }},

    {"$unwind": "$away_team_info"},

    {"$project": {"_id": 0,
                  "league": "$league_info.name",
                  "season": 1,
                  "home_team": "$home_team_info.team_long_name",
                  "away_team": "$away_team_info.team_long_name",
                  "odds_diff": 1    }}
                  
]



start_time = time.time()

result = db.matches.aggregate(pipeline)

df = pd.DataFrame.from_records(result) 

print()
print(tabulate(df, headers='keys', tablefmt="psql", numalign='center', stralign="center", showindex=False))

print(f"\n Execution time: {time.time() - start_time:.3f} seconds")