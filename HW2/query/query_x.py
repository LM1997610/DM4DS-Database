
from pymongo import MongoClient
import pandas as pd
from tabulate import tabulate
import time

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['soccer_db']

# find the teams with the highest number of goals scored in a specific season

pipeline = [
    {"$match": {"season": "2008/2009"}},

    {"$group": {"_id": "$home_team_api_id",
                "total_goals": {"$sum": "$home_team_goal"}      }},

    {"$lookup": { "from": "teams",
                  "localField": "_id",
                  "foreignField": "team_api_id",
                  "as": "team_info" }},

    {"$unwind": "$team_info"},
    {"$project": {"_id": 0,
                  "team_name": "$team_info.team_long_name",
                  "total_goals": 1  }},

    {"$sort": {"total_goals": -1}},
    {"$limit": 10}

]

start_time = time.time()

result = db.matches.aggregate(pipeline)


df = pd.DataFrame.from_records(result) 

print()
print(tabulate(df, headers='keys', tablefmt="github", numalign='center', stralign="center"))

print(f"\n Execution time: {time.time() - start_time:.3f} seconds")