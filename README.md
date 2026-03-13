# Global News Monitor

Global News Monitor is a Python project for monitoring global events from GDELT. The project started as a console-based fetch-and-print tool and is now being refactored into a small ingestion pipeline with PostgreSQL as the source of truth.

GDELT, the Global Database of Events, Language and Tone, is a large open dataset that extracts structured events from worldwide news coverage. It tracks actors, locations, event types, and sentiment across reporting from many countries and languages, and it updates continuously.

The current codebase still supports console summaries, and Stage 1 now adds the database foundation for persistent ingestion, export-level checkpointing, and deduplication.

## Features

- Fetch latest GDELT event dataset
- Extract key event fields
- Convert event codes into readable labels for console output
- Persist ingestion runs and raw events in PostgreSQL
- Track export checkpoints for future incremental ingestion

## Example Output

```text
Global News Monitor starting...

[EVENT] UNITED KINGDOM → PRESIDENT | Diplomatic consultation (043) | 2
[EVENT] PRESIDENT → UNITED STATES | Diplomatic consultation (043) | 1
```

## Why the Event Dataset Instead of the DOC API

Originally I attempted to use the GDELT DOC API for this project.

However, the DOC API is designed for searching news articles by keywords and tends to return article-level results rather than structured event data.

It also frequently returned HTTP 429 rate-limit errors during development.

The project switched to the official GDELT Event export dataset instead.

Advantages of the Event dataset:

- Updates every 15 minutes
- Contains structured event records
- Includes actor names, event codes, countries, and coordinates
- More stable for continuous monitoring
- Doesnt give me an error 35% of the time haha

This makes it much better for building a real-time event monitoring system.

## Tech Stack

- Python
- Requests
- PostgreSQL
- Psycopg
- GDELT Event dataset

## Project Structure

`src/main.py`
Entry point for both console monitoring and the new ingestion skeleton.

`src/gdelt_events.py`
Handles downloading the newest GDELT dataset and parsing the CSV export.

`src/db.py`
Creates PostgreSQL connections, loads `DATABASE_URL`, and provides transaction helpers.

`src/ingestion/repository.py`
Contains repository functions for ingestion runs, export checkpoints, and raw event inserts.

`sql/stage1_schema.sql`
PostgreSQL DDL for Stage 1 ingestion tables.

`tests/`
Contains unit tests for the project.

## PostgreSQL Setup

PostgreSQL is now required for the ingestion command.

Set the `DATABASE_URL` environment variable before running ingestion:

```bash
DATABASE_URL=postgresql://username:password@localhost:5432/global_news_monitor
```

If you prefer using a local `.env` file, `python-dotenv` is supported as an optional dependency.

Apply the schema before your first ingestion run:

```bash
psql "$DATABASE_URL" -f sql/stage1_schema.sql
```

## Running the Project

Run the original console monitor:

```bash
python -m src.main
```

Run the new ingestion skeleton:

```bash
python -m src.main ingest
```

## Running Tests

```bash
pytest
```

## Architecture Direction

Stage 1 focuses on:

- persistent ingestion metadata
- export-level checkpoint tracking
- raw event storage with database-enforced deduplication

Later stages can build clustering, scoring, APIs, and dashboards on top of this stored event history.
