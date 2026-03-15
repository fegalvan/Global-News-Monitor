# Global News Monitor

Global News Monitor is a Python project for monitoring global events from GDELT. The project started as a console-based fetch-and-print tool and is now being refactored into a small ingestion pipeline with PostgreSQL as the source of truth.

GDELT, the Global Database of Events, Language and Tone, is a large open dataset that extracts structured events from worldwide news coverage. It tracks actors, locations, event types, and sentiment across reporting from many countries and languages, and it updates continuously.

The current codebase still supports console summaries, and Stage 2 now connects the ingestion command to the latest GDELT 15-minute event export with PostgreSQL-backed checkpointing and deduplication.

## Features

- Fetch latest GDELT event dataset
- Extract key event fields
- Convert event codes into readable labels for console output
- Persist ingestion runs and raw events in PostgreSQL
- Track export checkpoints for future incremental ingestion
- Retry GDELT export discovery and downloads with backoff
- Reset stale checkpoints that got stuck in processing

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
- Tenacity
- PostgreSQL
- Psycopg
- GDELT Event dataset

## Project Structure

`src/main.py`
Thin CLI wrapper for console monitoring and PostgreSQL-backed ingestion.

`src/gdelt_events.py`
Handles export discovery, retrying GDELT downloads, and parsing the CSV export.

`src/pipeline/ingest_service.py`
Runs the ingestion workflow for the latest export or a specific export URL.

`src/domain/events/categorization.py`
Contains deterministic category rules used by both ingestion and console monitoring.

`src/db.py`
Creates PostgreSQL connections, loads `DATABASE_URL`, and provides transaction helpers.

`src/ingestion/repository.py`
Contains repository functions for ingestion runs, export checkpoints, and raw event inserts.

`sql/stage1_schema.sql`
PostgreSQL DDL for the ingestion tables required by the current pipeline.

`sql/stage2_schema.sql`
PostgreSQL DDL for the future normalized event layer.

`tests/`
Contains unit tests for the project.

## Ingestion Architecture

The ingestion flow is now split into layers so it can scale a little better:

1. `src/main.py` handles CLI commands.
2. `src/pipeline/ingest_service.py` runs the orchestration.
3. `src/gdelt_events.py` talks to GDELT and parses the export.
4. `src/ingestion/transform.py` normalizes raw rows for insert.
5. `src/domain/events/categorization.py` assigns first-class categories and confidence.
6. `src/ingestion/repository.py` writes checkpoints and events to PostgreSQL.

That means the CLI stays thin while the actual ingest logic can be reused later by schedulers, jobs, or an API.

```mermaid
flowchart LR
    A["python -m src.main ingest"] --> B["src/pipeline/ingest_service.py"]
    B --> C["src/gdelt_events.py (download + parse)"]
    C --> D["src/ingestion/transform.py"]
    D --> E["src/domain/events/categorization.py"]
    E --> F["src/ingestion/repository.py"]
    F --> G["raw_events"]
    F --> H["normalized_events"]
```

## Ingestion Flow

The ingestion service processes rows in chunks instead of loading one giant in-memory list.

1. Discover latest export URL from GDELT.
2. Claim checkpoint (or skip if already completed).
3. Stream rows from the export ZIP.
4. Normalize each row and categorize it.
5. Insert into `raw_events` (immutable landing table).
6. For rows that were newly inserted, insert into `normalized_events`.
7. Update checkpoint metrics and ingestion run status.

## How Checkpoints Work

Each GDELT export gets a checkpoint row in `gdelt_export_checkpoints`.

- `pending` means we discovered the export but have not started processing it yet.
- `processing` means a worker claimed it and is currently ingesting it.
- `completed` means the export was fully processed.
- `failed` means the export hit an error and can be retried later.

There is also stale checkpoint recovery now. If a checkpoint has been stuck in `processing` for more than 30 minutes, the ingestion service resets it back to `pending` before continuing.

## Raw Vs Normalized Events

`raw_events` is the landing table.

- it stores the original parsed fields
- it keeps `raw_payload` so we can always go back to the original row
- it is the safest place to preserve data while the schema is still evolving

`normalized_events` is the next layer.

- it is meant for cleaner backend and analytics queries
- it points back to `raw_events` with `raw_event_id`
- it is where a more stable event model can grow over time

The ingestion pipeline now writes to `raw_events` first and then writes category-aware rows to `normalized_events` for newly inserted raw rows.

## Category System (Cyber + Crisis)

Categories are first-class and deterministic:

- conflict
- protest
- politics
- diplomacy
- economics
- cyber
- crisis

`cyber` detection uses a mix of event-code signals and keyword signals like:

- cyber attack
- hacking
- ransomware
- breach

`crisis` detection uses keyword rules and subcategories:

- environmental
- humanitarian
- epidemic
- natural_disaster

Humanitarian event codes such as `023/024/025` are treated as weak signals only, not the entire crisis classifier by themselves.

## PostgreSQL Setup

PostgreSQL is required for `python -m src.main ingest`.

Set the `DATABASE_URL` environment variable before running ingestion:

```bash
DATABASE_URL=postgresql://username:password@localhost:5432/global_news_monitor
```

If you prefer using a local `.env` file, `python-dotenv` is supported as an optional dependency.

Install dependencies:

```bash
pip install -r requirements.txt
```

Apply the schema before your first ingestion run:

```bash
psql "$DATABASE_URL" -f sql/stage1_schema.sql
```

If you also want the future normalized table ready:

```bash
psql "$DATABASE_URL" -f sql/stage2_schema.sql
```

## Running the Project

Run the original console monitor. This remains the default behavior:

```bash
python -m src.main
```

Run ingestion for the latest GDELT event export. This requires `DATABASE_URL` and the schema to already be applied:

```bash
python -m src.main ingest
```

The ingest command still works the same, but now it goes through the service layer in `src/pipeline/ingest_service.py`.

## Running Tests

```bash
pytest
```

## Architecture Direction

The current ingestion stage focuses on:

- fetching the latest GDELT 15-minute event export
- persistent ingestion metadata
- export-level checkpoint tracking
- raw event storage with database-enforced deduplication

Later stages can build clustering, scoring, APIs, and dashboards on top of this stored event history.
