## This script is used to create the tables in the database

import os
from dotenv import load_dotenv
import psycopg

load_dotenv()

CONNECTION = os.getenv('DATABASE_URL')  # Read connection string from .env file

# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION IF NOT EXISTS vector"

# Create podcast table
CREATE_PODCAST_TABLE = """
    CREATE TABLE IF NOT EXISTS podcast (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL
    )
"""

# Create segment table with vector embedding
CREATE_SEGMENT_TABLE = """
    CREATE TABLE IF NOT EXISTS segment (
        id TEXT PRIMARY KEY,
        start_time FLOAT NOT NULL,
        end_time FLOAT NOT NULL,
        content TEXT NOT NULL,
        embedding VECTOR(128) NOT NULL,
        podcast_id TEXT NOT NULL,
        CONSTRAINT fk_podcast
            FOREIGN KEY (podcast_id)
            REFERENCES podcast(id)
            ON DELETE CASCADE
    )
"""

# Create indexes (run AFTER data insertion for better performance)
CREATE_INDEXES = """
    -- Vector similarity index using IVFFlat
    CREATE INDEX IF NOT EXISTS idx_segment_embedding
    ON segment USING ivfflat (embedding vector_l2_ops)
    WITH (lists = 100);

    -- Index on podcast_id for filtering
    CREATE INDEX IF NOT EXISTS idx_segment_podcast_id
    ON segment(podcast_id);
"""

if __name__ == "__main__":
    print("=" * 60)
    print("Building Database Schema")
    print("=" * 60)

    try:
        with psycopg.connect(CONNECTION) as conn:
            with conn.cursor() as cur:
                # Enable pgvector extension
                print("\n1. Enabling pgvector extension...")
                cur.execute(CREATE_EXTENSION)
                print("   ✓ pgvector extension enabled")

                # Create podcast table
                print("\n2. Creating podcast table...")
                cur.execute(CREATE_PODCAST_TABLE)
                print("   ✓ podcast table created")

                # Create segment table
                print("\n3. Creating segment table...")
                cur.execute(CREATE_SEGMENT_TABLE)
                print("   ✓ segment table created")

                print("\n" + "=" * 60)
                print("Note: Indexes should be created AFTER data insertion")
                print("      Uncomment CREATE_INDEXES section in db_insert.py")
                print("=" * 60)

            conn.commit()

        print("\n✓ Database build completed successfully!")

    except psycopg.OperationalError as e:
        print(f"\n✗ Connection failed: {e}")
        print("  Check your DATABASE_URL in .env file")
    except psycopg.ProgrammingError as e:
        print(f"\n✗ SQL error: {e}")
        print("  Check that pgvector extension is available")
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")


