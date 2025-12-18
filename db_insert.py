## This script is used to insert data into the database
import os
import json
from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd
import psycopg

from utils import fast_pg_insert

load_dotenv()

CONNECTION = os.getenv('DATABASE_URL')  # Read connection string from .env file

# TODO: Read the embedding files

# TODO: Read documents files

# HINT: In addition to the embedding and document files you likely need to load the raw data via the hugging face datasets library
ds = load_dataset("Whispering-GPT/lex-fridman-podcast")


# TODO: Insert into postgres
# HINT: use the utils.fast_pg_insert function to insert data into the database
# otherwise inserting the 800k documents will take a very, very long time