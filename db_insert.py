## This script is used to insert data into the database
import os
import json
import glob
from pathlib import Path
from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd
import psycopg

from utils import fast_pg_insert
from db_build import CREATE_INDEXES

load_dotenv()

CONNECTION = os.getenv('DATABASE_URL')  # Read connection string from .env file

def main():
    print("=" * 70)
    print("Podcast Recommendation System - Data Insertion")
    print("=" * 70)

    # ==================== PHASE 1: Load HuggingFace Dataset ====================
    print("\n[Phase 1/5] Loading HuggingFace dataset...")
    ds = load_dataset("Whispering-GPT/lex-fridman-podcast", split="train")
    print(f"✓ Loaded {len(ds)} podcasts from HuggingFace")

    # Create podcast mapping: podcast_idx → YouTube ID
    podcast_mapping = {}
    podcast_data = []

    for idx, row in enumerate(ds):
        youtube_id = row['id']
        title = row['title']

        podcast_mapping[idx] = youtube_id
        podcast_data.append({
            'id': youtube_id,
            'title': title
        })

    podcast_df = pd.DataFrame(podcast_data)
    print(f"✓ Created podcast mapping for {len(podcast_df)} podcasts")
    print(f"  Sample: podcast_idx 89 → YouTube ID '{podcast_mapping[89]}'")

    # ==================== PHASE 2: Load Batch Request Files ====================
    print("\n[Phase 2/5] Loading batch request files...")
    batch_files = sorted(glob.glob("data/documents/batch_request_*.jsonl"))
    print(f"Found {len(batch_files)} batch request files")

    segment_metadata = {}  # custom_id → metadata dict

    for batch_file in batch_files:
        filename = Path(batch_file).name
        print(f"  Processing {filename}...", end=" ")

        count = 0
        with open(batch_file, 'r') as f:
            for line in f:
                record = json.loads(line)
                custom_id = record['custom_id']

                # Extract metadata
                metadata = record['body']['metadata']
                segment_metadata[custom_id] = {
                    'content': record['body']['input'],
                    'start_time': metadata['start_time'],
                    'end_time': metadata['stop_time'],
                    'youtube_id': metadata['podcast_id']
                }
                count += 1

        print(f"✓ {count:,} segments")

    print(f"✓ Loaded {len(segment_metadata):,} segment metadata records")

    # ==================== PHASE 3: Load Embedding Files ====================
    print("\n[Phase 3/5] Loading embedding files...")
    embedding_files = sorted(glob.glob("data/embedding/*.jsonl"))
    print(f"Found {len(embedding_files)} embedding files")

    embeddings = {}  # custom_id → embedding vector

    for emb_file in embedding_files:
        filename = Path(emb_file).name
        print(f"  Processing {filename}...", end=" ")

        count = 0
        with open(emb_file, 'r') as f:
            for line in f:
                record = json.loads(line)
                custom_id = record['custom_id']

                # Extract embedding vector (128 dimensions)
                embedding_data = record['response']['body']['data'][0]
                embedding_vector = embedding_data['embedding']

                embeddings[custom_id] = embedding_vector
                count += 1

        print(f"✓ {count:,} embeddings")

    print(f"✓ Loaded {len(embeddings):,} embedding vectors")

    # ==================== PHASE 4: Correlate Data & Create DataFrames ====================
    print("\n[Phase 4/5] Correlating data sources...")

    segment_data = []
    missing_metadata = 0
    missing_podcast_idx = 0
    mismatched_youtube_id = 0
    invalid_custom_id = 0

    for custom_id, embedding_vector in embeddings.items():
        # Check if we have metadata for this segment
        if custom_id not in segment_metadata:
            missing_metadata += 1
            continue

        metadata = segment_metadata[custom_id]

        # Parse custom_id to get podcast_idx
        # Expected format: "podcast_idx:segment_idx" (e.g., "89:115")
        if ':' not in custom_id:
            invalid_custom_id += 1
            continue

        parts = custom_id.split(':')
        if len(parts) != 2:
            invalid_custom_id += 1
            continue

        podcast_idx_str, segment_idx_str = parts
        try:
            podcast_idx = int(podcast_idx_str)
        except ValueError:
            invalid_custom_id += 1
            continue

        # Look up YouTube ID from HuggingFace mapping
        if podcast_idx not in podcast_mapping:
            missing_podcast_idx += 1
            continue

        youtube_id = podcast_mapping[podcast_idx]

        # Verify YouTube ID consistency (optional validation)
        if metadata['youtube_id'] != youtube_id:
            mismatched_youtube_id += 1
            # Use HuggingFace mapping as authoritative source
            youtube_id = podcast_mapping[podcast_idx]

        # Create segment record
        segment_data.append({
            'id': custom_id,
            'start_time': metadata['start_time'],
            'end_time': metadata['end_time'],
            'content': metadata['content'],
            'embedding': str(embedding_vector),  # Convert to string for pgvector
            'podcast_id': youtube_id
        })

    segment_df = pd.DataFrame(segment_data)

    print(f"✓ Created {len(segment_df):,} segment records")
    print(f"\nData Quality Report:")
    print(f"  - Missing metadata: {missing_metadata:,}")
    print(f"  - Invalid custom_id format: {invalid_custom_id:,}")
    print(f"  - Missing podcast_idx in HuggingFace: {missing_podcast_idx:,}")
    print(f"  - YouTube ID mismatches (corrected): {mismatched_youtube_id:,}")
    print(f"  - Successfully correlated: {len(segment_df):,} / {len(embeddings):,}")

    # ==================== PHASE 5: Insert Data into PostgreSQL ====================
    print("\n[Phase 5/5] Inserting data into PostgreSQL...")

    # Validate foreign key integrity before insertion
    podcast_ids_in_segments = set(segment_df['podcast_id'].unique())
    podcast_ids_in_podcasts = set(podcast_df['id'].unique())
    missing_podcasts = podcast_ids_in_segments - podcast_ids_in_podcasts

    if missing_podcasts:
        print(f"\n⚠ Warning: {len(missing_podcasts)} podcast IDs in segments not found in podcasts table")
        print(f"  Example: {list(missing_podcasts)[:5]}")
    else:
        print("✓ All foreign key references validated")

    # Insert podcasts first (foreign key dependency)
    print("\n1. Inserting podcasts...")
    fast_pg_insert(
        df=podcast_df,
        connection=CONNECTION,
        table_name='podcast',
        columns=['id', 'title']
    )
    print(f"   ✓ Inserted {len(podcast_df):,} podcasts")

    # Insert segments
    print("\n2. Inserting segments (this may take 2-5 minutes)...")
    fast_pg_insert(
        df=segment_df,
        connection=CONNECTION,
        table_name='segment',
        columns=['id', 'start_time', 'end_time', 'content', 'embedding', 'podcast_id']
    )
    print(f"   ✓ Inserted {len(segment_df):,} segments")

    # Create indexes
    print("\n3. Creating indexes (this may take 5-10 minutes)...")
    print("   Creating vector similarity index (IVFFlat)...")
    try:
        with psycopg.connect(CONNECTION) as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_INDEXES)
            conn.commit()
        print("   ✓ All indexes created successfully")
    except Exception as e:
        print(f"   ⚠ Index creation failed: {e}")
        print("   You can create indexes manually later using db_build.CREATE_INDEXES")

    # Final verification
    print("\n" + "=" * 70)
    print("Data Insertion Complete - Verification")
    print("=" * 70)

    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM podcast")
            podcast_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM segment")
            segment_count = cur.fetchone()[0]

    print(f"  Podcasts in database: {podcast_count:,}")
    print(f"  Segments in database: {segment_count:,}")

    # Test key segments exist
    test_segments = ["267:476", "48:511", "51:56"]
    print(f"\nVerifying key test segments:")
    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor() as cur:
            for seg_id in test_segments:
                cur.execute("SELECT content FROM segment WHERE id = %s", (seg_id,))
                result = cur.fetchone()
                if result:
                    content_preview = result[0][:50] + "..." if len(result[0]) > 50 else result[0]
                    print(f"  ✓ Segment {seg_id}: \"{content_preview}\"")
                else:
                    print(f"  ✗ Segment {seg_id}: NOT FOUND")

    print("\n" + "=" * 70)
    print("✓ All data successfully loaded!")
    print("  Next step: Run db_query.py to generate similarity search results")
    print("=" * 70)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n✗ Error during data insertion: {e}")
        import traceback
        traceback.print_exc()