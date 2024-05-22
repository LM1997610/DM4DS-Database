import time
import pandas as pd

from tabulate import tabulate
from pymongo import MongoClient


client = MongoClient('mongodb://localhost:27017/')
db = client['soccer_db']

pipeline = [


]



start_time = time.time()   

result = list(db.leagues.aggregate(pipeline))

df = pd.DataFrame.from_records(result) 

print()
print(tabulate(df, headers='keys', tablefmt="github", numalign='center', stralign="center"))

print(f"\n Execution time: {time.time() - start_time:.3f} seconds")