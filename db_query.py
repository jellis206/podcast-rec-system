## This script is used to query the database
import os
import psycopg
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

CONNECTION = os.getenv('DATABASE_URL')  # Read connection string from .env file

# ==================== Query Functions ====================

def query_similar_segments(segment_id: str, limit: int = 5):
    """Find most similar segments using L2 distance."""
    query = """
        SELECT
            p.title as podcast_name,
            s.id as segment_id,
            s.content as segment_text,
            s.start_time,
            s.end_time,
            s.embedding <-> (
                SELECT embedding
                FROM segment
                WHERE id = %s
            ) as distance
        FROM segment s
        JOIN podcast p ON s.podcast_id = p.id
        WHERE s.id != %s
        ORDER BY distance ASC
        LIMIT %s
    """

    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (segment_id, segment_id, limit))
            results = cur.fetchall()

    return results

def query_dissimilar_segments(segment_id: str, limit: int = 5):
    """Find most dissimilar segments using L2 distance."""
    query = """
        SELECT
            p.title as podcast_name,
            s.id as segment_id,
            s.content as segment_text,
            s.start_time,
            s.end_time,
            s.embedding <-> (
                SELECT embedding
                FROM segment
                WHERE id = %s
            ) as distance
        FROM segment s
        JOIN podcast p ON s.podcast_id = p.id
        WHERE s.id != %s
        ORDER BY distance DESC  -- Descending for most dissimilar
        LIMIT %s
    """

    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (segment_id, segment_id, limit))
            results = cur.fetchall()

    return results

def query_similar_episodes_by_segment(segment_id: str, limit: int = 5):
    """
    Find most similar podcast episodes to a given segment.
    Method: Compare segment embedding to average embedding of each episode.
    """
    query = """
        WITH query_embedding AS (
            -- Get the query segment's embedding
            SELECT embedding, podcast_id as query_podcast_id
            FROM segment
            WHERE id = %s
        ),
        episode_embeddings AS (
            -- Calculate average embedding for each podcast episode
            SELECT
                podcast_id,
                AVG(embedding) as avg_embedding
            FROM segment
            GROUP BY podcast_id
        )
        SELECT
            p.title as podcast_title,
            ee.avg_embedding <-> (SELECT embedding FROM query_embedding) as distance
        FROM episode_embeddings ee
        JOIN podcast p ON ee.podcast_id = p.id
        WHERE ee.podcast_id != (SELECT query_podcast_id FROM query_embedding)
        ORDER BY distance ASC
        LIMIT %s
    """

    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (segment_id, limit))
            results = cur.fetchall()

    return results

def query_similar_episodes_by_episode(podcast_id: str, limit: int = 5):
    """
    Find most similar podcast episodes to a given episode.
    Method: Compare average embeddings of episodes.
    """
    query = """
        WITH episode_embeddings AS (
            -- Calculate average embedding for each podcast episode
            SELECT
                podcast_id,
                AVG(embedding) as avg_embedding
            FROM segment
            GROUP BY podcast_id
        ),
        query_episode_embedding AS (
            -- Get the query episode's average embedding
            SELECT avg_embedding
            FROM episode_embeddings
            WHERE podcast_id = %s
        )
        SELECT
            p.title as podcast_title,
            ee.avg_embedding <-> (SELECT avg_embedding FROM query_episode_embedding) as distance
        FROM episode_embeddings ee
        JOIN podcast p ON ee.podcast_id = p.id
        WHERE ee.podcast_id != %s  -- Exclude query episode
        ORDER BY distance ASC
        LIMIT %s
    """

    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (podcast_id, podcast_id, limit))
            results = cur.fetchall()

    return results

def get_segment_text(segment_id: str) -> str:
    """Retrieve segment text for markdown headers."""
    query = "SELECT content FROM segment WHERE id = %s"
    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (segment_id,))
            result = cur.fetchone()
            return result[0] if result else ""

# ==================== Formatting Functions ====================

def format_segment_results(results, query_text: str, query_id: str, title: str):
    """Format segment query results as markdown table."""
    output = f"## {title}\n\n"
    output += f"**Query Segment ({query_id})**: \"{query_text}\"\n\n"
    output += "| Rank | Podcast Name | Segment ID | Start Time | End Time | Distance | Segment Text |\n"
    output += "|------|--------------|------------|------------|----------|----------|---------------|\n"

    for rank, row in enumerate(results, 1):
        podcast_name = row[0]
        segment_id = row[1]
        segment_text = row[2][:80] + "..." if len(row[2]) > 80 else row[2]
        # Escape pipe characters in text to avoid breaking markdown table
        segment_text = segment_text.replace('|', '\\|')
        podcast_name = podcast_name.replace('|', '\\|')
        start_time = f"{row[3]:.2f}"
        end_time = f"{row[4]:.2f}"
        distance = f"{row[5]:.4f}"

        output += f"| {rank} | {podcast_name} | {segment_id} | {start_time} | {end_time} | {distance} | {segment_text} |\n"

    output += "\n---\n\n"
    return output

def format_episode_results(results, title: str, query_info: str = ""):
    """Format episode query results as markdown table."""
    output = f"## {title}\n\n"
    if query_info:
        output += f"{query_info}\n\n"

    output += "| Rank | Podcast Title | Distance |\n"
    output += "|------|---------------|----------|\n"

    for rank, row in enumerate(results, 1):
        podcast_title = row[0].replace('|', '\\|')  # Escape pipes
        distance = f"{row[1]:.4f}"

        output += f"| {rank} | {podcast_title} | {distance} |\n"

    output += "\n---\n\n"
    return output

# ==================== Main Results Generation ====================

def generate_results_file():
    """Generate complete markdown results file."""
    print("=" * 70)
    print("Podcast Recommendation System - Query Execution")
    print("=" * 70)

    output = "# Podcast Recommendation System - Query Results\n\n"
    output += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    output += "All queries use L2 distance (`<->` operator) with query documents excluded from results.\n\n"
    output += "---\n\n"

    # Q1: Similar to "267:476"
    print("\nExecuting Q1: Most similar segments to '267:476'...")
    results = query_similar_segments("267:476", 5)
    query_text = get_segment_text("267:476")
    output += format_segment_results(results, query_text, "267:476",
                                     "Q1: Most Similar Segments to \"267:476\"")
    print(f"  ✓ Found {len(results)} results")

    # Q2: Dissimilar to "267:476"
    print("Executing Q2: Most dissimilar segments to '267:476'...")
    results = query_dissimilar_segments("267:476", 5)
    output += format_segment_results(results, query_text, "267:476",
                                     "Q2: Most Dissimilar Segments to \"267:476\"")
    print(f"  ✓ Found {len(results)} results")

    # Q3: Similar to "48:511"
    print("Executing Q3: Most similar segments to '48:511'...")
    results = query_similar_segments("48:511", 5)
    query_text = get_segment_text("48:511")
    output += format_segment_results(results, query_text, "48:511",
                                     "Q3: Most Similar Segments to \"48:511\"")
    print(f"  ✓ Found {len(results)} results")

    # Q4: Similar to "51:56"
    print("Executing Q4: Most similar segments to '51:56'...")
    results = query_similar_segments("51:56", 5)
    query_text = get_segment_text("51:56")
    output += format_segment_results(results, query_text, "51:56",
                                     "Q4: Most Similar Segments to \"51:56\"")
    print(f"  ✓ Found {len(results)} results")

    # Q5a-c: Episode similarity by segment
    output += "# Q5: Most Similar Episodes by Segment\n\n"
    output += "Method: Compare segment embedding to average episode embeddings.\n\n"
    output += "---\n\n"

    print("\nExecuting Q5a: Similar episodes to segment '267:476'...")
    results = query_similar_episodes_by_segment("267:476", 5)
    query_text = get_segment_text("267:476")
    output += format_episode_results(results,
                                     "Q5a: Similar Episodes to Segment \"267:476\"",
                                     f"**Query Segment**: \"{query_text}\"")
    print(f"  ✓ Found {len(results)} results")

    print("Executing Q5b: Similar episodes to segment '48:511'...")
    results = query_similar_episodes_by_segment("48:511", 5)
    query_text = get_segment_text("48:511")
    output += format_episode_results(results,
                                     "Q5b: Similar Episodes to Segment \"48:511\"",
                                     f"**Query Segment**: \"{query_text}\"")
    print(f"  ✓ Found {len(results)} results")

    print("Executing Q5c: Similar episodes to segment '51:56'...")
    results = query_similar_episodes_by_segment("51:56", 5)
    query_text = get_segment_text("51:56")
    output += format_episode_results(results,
                                     "Q5c: Similar Episodes to Segment \"51:56\"",
                                     f"**Query Segment**: \"{query_text}\"")
    print(f"  ✓ Found {len(results)} results")

    # Q6: Episode similarity by episode
    print("\nExecuting Q6: Similar episodes to 'VeH7qKZr0WI' (Balaji Srinivasan)...")
    results = query_similar_episodes_by_episode("VeH7qKZr0WI", 5)
    output += format_episode_results(results,
                                     "Q6: Most Similar Episodes to \"VeH7qKZr0WI\"",
                                     "**Query Episode**: Balaji Srinivasan: How to Fix Government, Twitter, Science, and the FDA | Lex Fridman Podcast #331")
    print(f"  ✓ Found {len(results)} results")

    # Add notes section
    output += "# Notes\n\n"
    output += "- **Distance Metric**: L2 (Euclidean) distance in 128-dimensional space\n"
    output += "- **Smaller distance values** indicate higher similarity\n"
    output += "- **Query documents excluded** from all result sets\n"
    output += "- **Episode similarity** computed using averaged embeddings across all segments\n"

    # Write to file
    with open('results/queries.md', 'w') as f:
        f.write(output)

    print("\n" + "=" * 70)
    print("✓ Results written to results/queries.md")
    print("=" * 70)

# ==================== Main Execution ====================

if __name__ == "__main__":
    print("=" * 70)
    print("Podcast Recommendation System: Query Execution")
    print("=" * 70)

    try:
        # Test database connection
        print("\nConnecting to database...")
        with psycopg.connect(CONNECTION) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM podcast")
                podcast_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM segment")
                segment_count = cur.fetchone()[0]

        print(f"✓ Connected to database")
        print(f"  Podcasts: {podcast_count:,}")
        print(f"  Segments: {segment_count:,}")

        # Generate all results
        generate_results_file()

        print("\n✓ All queries completed successfully!")
        print("  View results in: results/queries.md")

    except psycopg.OperationalError as e:
        print(f"\n✗ Connection failed: {e}")
        print("  Check your DATABASE_URL in .env file")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()

