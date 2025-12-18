## This script is used to query the database
import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

CONNECTION = os.getenv('DATABASE_URL')  # Read connection string from .env file

# TODO: Write queries for Q1-Q6
# Example query pattern with psycopg3:
# with psycopg.connect(CONNECTION) as conn:
#     with conn.cursor() as cur:
#         cur.execute("SELECT * FROM segment WHERE id = %s", (segment_id,))
#         results = cur.fetchall()

