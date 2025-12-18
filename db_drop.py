## This script is used to drop the tables in the database

import os
from dotenv import load_dotenv
import psycopg

load_dotenv()

CONNECTION = os.getenv('DATABASE_URL')  # Read connection string from .env file

DROP_TABLE = "DROP TABLE IF EXISTS segment, podcast CASCADE"

with psycopg.connect(CONNECTION) as conn:
    with conn.cursor() as cur:
        cur.execute(DROP_TABLE)
    conn.commit()
    print("Tables dropped successfully!")