
import time
import pandas as pd

from tabulate import tabulate
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['soccer_db']


pipeline = [
   
    {"$lookup": {"from": "matches",
                 "localField": "team_api_id",
                 "foreignField": "away_team_api_id",
                 "as": "match_info"  }},

    {"$unwind": "$match_info"},
   
    {"$group": {"_id": "$team_long_name",
                "conceded_goals": {"$sum": "$match_info.home_team_goal"},
                "num_matches": {"$count": {}}   }},

    {"$sort": {"conceded_goals": -1}},
    
    {"$limit": 10},
    
    {"$project": {"_id": 0,
                  "team_name": "$_id",
                  "conceded_goals": 1,
                  "num_matches": 1  }}
]

start_time = time.time()

result = db.teams.aggregate(pipeline)


df = pd.DataFrame.from_records(result) 

print()
print(tabulate(df, headers='keys', tablefmt="psql", numalign='center', stralign="center"))

print(f"\n Execution time: {time.time() - start_time:.3f} seconds")

