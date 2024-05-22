
import time
import pandas as pd

from tabulate import tabulate
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['soccer_db']


pipeline = [

    {"$match": {"season": "2008/2009"}  },

    {"$lookup": {"from": "leagues",
                 "localField": "league_id",
                 "foreignField": "id",
                 "as": "league"}    },

    {"$unwind": "$league"   },

    {"$lookup": {"from": "players",
                 "localField": "home_player_1",
                 "foreignField": "player_api_id",
                 "as": "home_players"}  },

    {"$lookup": {"from": "players",
                 "localField": "away_player_1",
                 "foreignField": "player_api_id",
                 "as": "away_players"   }   },

    {"$project": {"league_name": "$league.name",
                  "players": {"$setUnion": [
                      {"$map": {"input": ["$home_player_1", "$home_player_2", "$home_player_3", "$home_player_4", "$home_player_5", "$home_player_6", "$home_player_7", "$home_player_8", "$home_player_9", "$home_player_10", "$home_player_11"],
                        "as": "home_player_id",
                        "in": {"$arrayElemAt": ["$home_players", {"$indexOfArray": ["$home_players.player_api_id", "$$home_player_id"]}]    }}},

                      {"$map": {"input": ["$away_player_1", "$away_player_2", "$away_player_3", "$away_player_4", "$away_player_5", "$away_player_6", "$away_player_7", "$away_player_8", "$away_player_9", "$away_player_10", "$away_player_11"],
                        "as": "away_player_id",
                        "in": {"$arrayElemAt": ["$away_players", {"$indexOfArray": ["$away_players.player_api_id", "$$away_player_id"]}]    }}}
                ]}  }   },

    {"$unwind": "$players"},

    {"$group": {"_id": {"league_name": "$league_name", "player_id": "$players.player_api_id"},
                "player_name": {"$first": "$players.player_name"},
                "player_height": {"$first": "$players.height"}}     },

    {"$group": {"_id": "$_id.league_name",
                "max_height": {"$max": "$player_height"}}   },

    {"$lookup": {"from": "players",
                 "localField": "max_height",
                 "foreignField": "height",
                 "as": "tallest_players"}   },

    {"$unwind": "$tallest_players"  },

    {"$project": {"_id": 0,
                  "league_name": "$_id",
                  "player_name": "$tallest_players.player_name",
                  "player_height": "$max_height"}      },

    {"$sort": {"league_name": 1}    }

]




start_time = time.time()

result = list(db.matches.aggregate(pipeline))

df = pd.DataFrame.from_records(result) 

print()
print(tabulate(df, headers='keys', tablefmt="github", numalign='center', stralign="center"))

print(f"\n Execution time: {time.time() - start_time:.3f} seconds")