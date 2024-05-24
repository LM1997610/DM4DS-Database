
import time
import pandas as pd

from tabulate import tabulate
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['soccer_db']

pipeline = [

    {"$match": {"$expr": {"$eq": ["$home_team_goal", "$away_team_goal"]}}   },

    {"$facet": {"home_draws": [{"$group": {"_id": "$home_team_api_id", "num_draws": {"$sum": 1}    }   }   ],
                "away_draws": [{"$group": {"_id": "$away_team_api_id", "num_draws": {"$sum": 1}    }   }   ],   }   },

    {"$project": {"draw_matches": {"$setUnion": ["$home_draws", "$away_draws"]}}   },

    {"$unwind": "$draw_matches"},

    {"$group": {"_id": "$draw_matches._id", "draws": {"$sum": "$draw_matches.num_draws"}    }   },
    
    {"$unionWith": {"coll": "matches",
                    "pipeline": [{"$match": {"$expr": {"$gt": ["$home_team_goal", "$away_team_goal"]}}},
                                 {"$group": {"_id": "$home_team_api_id", "num_wins": {"$sum": 1}}}      ]   }   },

    {"$unionWith": {"coll": "matches",
                    "pipeline": [{"$match": {"$expr": {"$lt": ["$home_team_goal", "$away_team_goal"]}}},
                                 {"$group": {"_id": "$away_team_api_id", "num_wins": {"$sum": 1}}}      ]   }   },

    {"$group": {"_id": "$_id",
                "draws": {"$sum": "$draws"},
                "wins": {"$sum": "$num_wins"}   }   },
    
    {"$unionWith": {"coll": "matches",
                    "pipeline": [{"$match": {"$expr": {"$lt": ["$home_team_goal", "$away_team_goal"]}}},
                                 {"$group": {"_id": "$home_team_api_id", "num_loses": {"$sum": 1}}}     ]   }   },
    {"$unionWith": {"coll": "matches",
                    "pipeline": [{"$match": {"$expr": {"$gt": ["$home_team_goal", "$away_team_goal"]}}},
                                 {"$group": {"_id": "$away_team_api_id", "num_loses": {"$sum": 1}}}     ]   }   },

    {"$group": {"_id": "$_id",
                "draws": {"$sum": "$draws"},
                "wins": {"$sum": "$wins"},
                "loses": {"$sum": "$num_loses"}}    },
    
    {"$lookup": {"from": "teams",
                 "localField": "_id",
                 "foreignField": "team_api_id",
                 "as": "team"}  },

    {"$unwind": "$team" },
    
    {"$project": {"_id": 0,
                  "team_long_name": "$team.team_long_name",
                  "points": {"$add": [{"$multiply": ["$wins", 3]}, "$draws"]}   }   },
    
    {"$sort": {"points": -1}    },
    {"$limit": 10   }

]


start_time = time.time()

result = list(db.matches.aggregate(pipeline))

df = pd.DataFrame.from_records(result) 

print()
print(tabulate(df, headers='keys', tablefmt="psql", numalign='center', stralign="center"))

print(f"\n Execution time: {time.time() - start_time:.3f} seconds")