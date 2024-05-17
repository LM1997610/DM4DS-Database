
import os
import json
import psycopg2

import config

from tqdm.auto import tqdm

def fetch_data(query, conn):
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    result = [dict(zip(columns, row)) for row in data]
    return result

def export_to_json(conn, table_name):
    query = f"SELECT * FROM {table_name};"
    data = fetch_data(query, conn)

    output_folder = "db_converted"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    output_path = os.path.join(output_folder, f"{table_name}.json")

    with open(output_path, "w") as json_file:
        json.dump(data, json_file, default=str)


conn = psycopg2.connect(dbname = "soccer_database",
                        user = config.username,
                        password = config.password,
                        host = config.host,
                        port = config.port)

tables = ["country", "league", "team", "player", "match", "team_attributes", "player_attributes"]
    
for table in tqdm(tables):
    export_to_json(conn, table)
    tqdm.write(f' table {table.capitalize()} done')

conn.close()
