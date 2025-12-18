## This script is used to create the tables in the database

import os
from dotenv import load_dotenv
import psycopg

load_dotenv()

CONNECTION = os.getenv('DATABASE_URL')  # Read connection string from .env file

# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION IF NOT EXISTS vector"

# TODO: Add create table statement
CREATE_PODCAST_TABLE = """

"""
# TODO: Add create table statement
CREATE_SEGMENT_TABLE = """

"""

# TODO: Create tables with psycopg3
# Example usage:
# with psycopg.connect(CONNECTION) as conn:
#     with conn.cursor() as cur:
#         cur.execute(CREATE_EXTENSION)
#         cur.execute(CREATE_PODCAST_TABLE)
#         cur.execute(CREATE_SEGMENT_TABLE)
#     conn.commit()


