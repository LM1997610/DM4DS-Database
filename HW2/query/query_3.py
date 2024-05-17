
import time
import pandas as pd

from tabulate import tabulate
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['soccer_db']

pipeline = []


result = list(db.matches.aggregate(pipeline))
print(result)
