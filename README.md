# Podcast Recommendation System

A vector database-powered recommendation system for Lex Fridman podcast episodes using PostgreSQL with the pgvector extension.

## Overview

This project builds a podcast recommendation system that uses vector embeddings to find similar podcast segments and episodes. The system leverages:

- **PostgreSQL with pgvector** extension for efficient vector storage and similarity search
- **OpenAI embeddings** (128-dimensional vectors) for semantic representation of podcast segments
- **Lex Fridman Podcast dataset** containing 346 podcasts and 832,839 segments

## Prerequisites

- Python 3.8+
- PostgreSQL instance with pgvector extension (TimescaleDB 30-day free trial recommended)
- Internet connection for downloading dataset and embeddings

## Setup Instructions

### 1. Create Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Set Up PostgreSQL Database

Sign up for a [TimescaleDB free trial](https://www.timescale.com/) (or use your own PostgreSQL instance with pgvector extension).

Create a `.env` file in the project root with your database connection string:

```
DATABASE_URL=postgresql://username:password@host:port/database
```

### 4. Download Dataset and Embeddings

You'll need three data sources:

1. **Raw podcast transcripts** (batch_request_*.jsonl files) - [Download here](https://huggingface.co/datasets/Whispering-GPT/lex-fridman-podcast)
2. **Vector embeddings** (embeddings.jsonl) - [Download here](https://platform.openai.com/docs/guides/embeddings)
3. **HuggingFace dataset** (loaded automatically via the `datasets` library)

Place the downloaded files in a `data/` directory:

```
data/
├── batch_request_00.jsonl
├── batch_request_01.jsonl
├── ...
└── embeddings.jsonl
```

## Database Schema

### `podcast` Table

| Column | Type | Description |
|--------|------|-------------|
| id | TEXT (PK) | Unique podcast ID from HuggingFace (e.g., "TRdL6ZzWBS0") |
| title | TEXT | Full podcast title |

### `segment` Table

| Column | Type | Description |
|--------|------|-------------|
| id | TEXT (PK) | Unique segment identifier (format: "podcast_idx;segment_idx") |
| start_time | FLOAT | Segment start timestamp |
| end_time | FLOAT | Segment end timestamp |
| content | TEXT | Raw text transcription |
| embedding | VECTOR(128) | 128-dimensional embedding vector |
| podcast_id | TEXT (FK) | Foreign key to podcast.id |

## Usage

### Step 1: Build Database Tables

Create the necessary database tables and enable the pgvector extension:

```bash
python db_build.py
```

This script will:
- Enable the pgvector extension
- Create the `podcast` table
- Create the `segment` table with vector support

### Step 2: Insert Data

Populate the database with podcast data and embeddings:

```bash
python db_insert.py
```

This script will:
- Read batch_request_*.jsonl files for podcast metadata
- Read embeddings.jsonl for vector embeddings
- Load the HuggingFace dataset for additional metadata
- Insert ~346 podcasts into the `podcast` table
- Insert ~832,839 segments into the `segment` table

**Note**: This process uses the optimized `fast_pg_insert` function for efficient bulk insertion.

### Step 3: Run Queries

Execute similarity search queries:

```bash
python db_query.py
```

## Query Examples

The system answers the following questions using L2 distance for similarity:

### Q1-Q4: Find Similar Segments

Find the 5 most similar (or dissimilar) segments to a given query segment:

- **Q1**: Most similar segments to "267:476" (about meeting alien life)
- **Q2**: Most dissimilar segments to "267:476"
- **Q3**: Most similar segments to "48:511" (about deep learning and neural networks)
- **Q4**: Most similar segments to "51:56" (about dark energy)

Returns: podcast name, segment ID, text, start/end time, embedding distance

### Q5: Find Similar Episodes by Segment

For given segments, find the 5 most similar podcast episodes by averaging embeddings:

- Segment "267:476"
- Segment "48:511"
- Segment "51:56"

Returns: Podcast title, embedding distance

### Q6: Find Similar Episodes by Episode

Find the 5 most similar episodes to "VeH7qKZr0WI" (Balaji Srinivasan episode)

Returns: Podcast title, embedding distance

## Vector Distance Functions

pgvector supports the following distance functions:

- `<->` - L2 distance (Euclidean)
- `<#>` - (negative) inner product
- `<=>` - cosine distance
- `<+>` - L1 distance (Manhattan)

This project uses L2 distance for all queries.

## Project Structure

```
podcast-rec-system/
├── db_build.py          # Create database tables
├── db_insert.py         # Insert podcast data
├── db_query.py          # Query similar segments/episodes
├── db_drop.py           # Drop tables (utility)
├── utils.py             # Helper functions (fast_pg_insert)
├── requirements.txt     # Python dependencies
├── .env                 # Database connection (not tracked)
├── README.md            # This file
└── data/                # Downloaded datasets (not tracked)
    ├── batch_request_*.jsonl
    └── embeddings.jsonl
```

## Deliverables

All results and documentation are provided in **Markdown format**:

- `results/queries.md` - Query results for Q1-Q6
- `results/analysis.md` - Analysis and insights (optional)

Convert to PDF later if needed using tools like Pandoc:

```bash
pandoc results/queries.md -o results/queries.pdf
```

## Resources

- [pgvector Documentation](https://github.com/pgvector/pgvector)
- [Lex Fridman Dataset on HuggingFace](https://huggingface.co/datasets/Whispering-GPT/lex-fridman-podcast)
- [OpenAI Embeddings API](https://platform.openai.com/docs/guides/embeddings)
- [TimescaleDB](https://www.timescale.com/)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)

## Troubleshooting

### Connection Issues

- Verify your `.env` file contains the correct DATABASE_URL
- Ensure your PostgreSQL instance is running and accessible
- Check that pgvector extension is installed on your PostgreSQL instance

### Import Errors

- Make sure your virtual environment is activated
- Reinstall dependencies: `pip install -r requirements.txt`

### Slow Insertions

- The `fast_pg_insert` function should be used for bulk insertions
- Avoid using individual INSERT statements for 800k+ records
- Consider creating indexes after insertion completes

## License

MIT License - See LICENSE file for details

## Acknowledgments

- Lex Fridman for the podcast dataset
- OpenAI for embeddings API
- pgvector team for the PostgreSQL extension
