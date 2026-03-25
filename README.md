# Global News Monitor

Global News Monitor is a Python project for pulling structured world event data from GDELT, classifying it into a small set of first-class categories, and storing it in PostgreSQL for inspection and analysis.

The repo currently supports two main workflows:

- a live console monitor that fetches the latest event feed and prints a human-readable summary
- a database-backed ingestion pipeline that checkpoints GDELT exports, deduplicates events, and writes both raw and normalized records for later querying

## What The Project Does Today

- fetches the latest GDELT Event export
- parses zipped GDELT rows into Python dictionaries
- normalizes events into a consistent insert shape
- assigns deterministic categories such as `diplomacy`, `politics`, `conflict`, `economics`, `protest`, `cyber`, and `crisis`
- stores ingestion runs and export checkpoints in PostgreSQL
- writes raw landing rows and normalized analytical rows
- deduplicates events by deterministic key
- exposes CLI commands for ingesting, migrating, and inspecting recent data quality
- exposes a FastAPI read API for the main analytics views

## Why GDELT Event Exports

This project uses the GDELT Event export files instead of the DOC API.

Why:

- the event exports are structured event data, not just article search results
- they update every 15 minutes
- they are a much better fit for repeated ingestion and checkpointing
- they are more reliable for this use case than keyword-based DOC API requests

## Current Architecture

The codebase is organized into a few clear layers:

1. `src/main.py` provides the CLI commands.
2. `src/connectors/gdelt/` discovers and parses GDELT export files.
3. `src/ingestion/transform.py` normalizes parsed rows and builds dedupe keys.
4. `src/domain/events/categorization.py` applies deterministic category rules.
5. `src/ingestion/repository.py` handles inserts, checkpoint state, and query helpers.
6. `src/pipeline/ingest_service.py` orchestrates the full ingestion flow.

```mermaid
flowchart LR
    A["python -m src.main ingest"] --> B["ingest_service"]
    B --> C["GDELT export discovery + download"]
    C --> D["row parsing"]
    D --> E["normalization + dedupe key"]
    E --> F["category rules"]
    F --> G["raw_events"]
    F --> H["normalized_events"]
    G --> I["stats / latest / runs"]
    H --> I
```

## Data Model

The PostgreSQL side is split into operational and analytical tables.

### Operational tables

- `ingestion_runs`: audit trail for each ingestion attempt
- `gdelt_export_checkpoints`: export-level checkpoint tracking with statuses like `pending`, `processing`, `completed`, and `failed`

### Event tables

- `raw_events`: landing table that preserves the original parsed payload and core fields
- `normalized_events`: cleaner analytical table with category, score, and query-friendly columns

The ingestion pipeline writes to `raw_events` first, then inserts linked rows into `normalized_events` for newly inserted raw records.

## Categorization Model

The project uses deterministic category rules instead of an LLM or ML model.

Top-level categories:

- `diplomacy`
- `politics`
- `conflict`
- `economics`
- `protest`
- `cyber`
- `crisis`

How it works:

- most events map from the 2-digit GDELT root event code
- `cyber` can override root mapping when strong event codes or cyber keywords are detected
- `crisis` can override root mapping when crisis keywords are detected
- crisis subcategories currently include `environmental`, `humanitarian`, `epidemic`, and `natural_disaster`
- if an event code is truly unknown, the classifier still returns a safe fallback category so downstream code never gets a null category

## CLI Commands

### Live console monitor

Fetch the latest feed and print grouped events:

```bash
python -m src.main
```

This is the original monitor mode. It is useful for quick eyeballing of what is happening in the latest GDELT slice.

### Ingest latest export

Download the latest GDELT export and write it to PostgreSQL:

```bash
python -m src.main ingest
```

This command:

1. discovers the latest export
2. creates an ingestion run
3. claims or skips the export checkpoint
4. parses rows in chunks
5. normalizes and categorizes rows
6. bulk inserts raw and normalized records
7. updates checkpoint and run metrics

### Run migrations

Apply all Alembic migrations:

```bash
python -m src.main migrate
```

### Inspect latest normalized rows

```bash
python -m src.main latest --limit 20
```

### Inspect recent ingestion runs

```bash
python -m src.main runs --limit 10
```

### Inspect recent aggregate stats

```bash
python -m src.main stats --hours 24
```

The `stats` command summarizes:

- total events in the time window
- missing actor percentage
- missing geo percentage
- unknown country percentage
- translation coverage by exact vs root event mapping
- fallback category usage
- top categories
- top countries

### Run the read API

Start the API locally with:

```bash
uvicorn src.api.main:app --reload
```

Available endpoints:

- `GET /health`
- `GET /latest`
- `GET /stats`
- `GET /spikes`
- `GET /tension`

## Local Setup

### Requirements

- Python 3.12+
- PostgreSQL

Install dependencies:

```bash
pip install -r requirements.txt
```

This includes the API dependencies `fastapi` and `uvicorn`.

### Environment Configuration

Create a repo-root `.env` file:

```env
DATABASE_URL=postgresql://username:password@localhost:5432/global_news_monitor
```

Notes:

- `ingest` reads `DATABASE_URL` from `.env`
- `migrate` also reads the same `.env`
- the Postgres integration tests now read the same `.env` too

That means reopening VS Code should not require re-exporting `DATABASE_URL` every time, as long as you run commands from the repository root.

### Database Setup

Run migrations before the first ingest:

```bash
python -m src.main migrate
```

Then ingest:

```bash
python -m src.main ingest
```

Manual SQL fallback:

```bash
psql "$DATABASE_URL" -f sql/stage1_schema.sql
psql "$DATABASE_URL" -f sql/stage2_schema.sql
psql "$DATABASE_URL" -f sql/stage3_indexes.sql
```

## How Ingestion Works

The ingestion service is designed to be rerunnable and safe for repeated polling.

Key behaviors:

- export checkpointing prevents the same GDELT export from being processed repeatedly
- stale `processing` checkpoints can be reset if they were abandoned
- events are deduplicated using a deterministic key
- raw and normalized inserts happen in bulk batches for speed
- the pipeline records inserted vs duplicated counts for each run

If `GLOBALEVENTID` exists, it is used for the dedupe key. Otherwise the pipeline falls back to a hash built from normalized event fields.

## Repository Layout

`src/main.py`
CLI entrypoint for monitor, ingest, migrate, latest, runs, and stats commands.

`src/db.py`
Database connection helpers and `.env`-aware `DATABASE_URL` loading.

`src/connectors/gdelt/export_client.py`
Discovers the latest export and downloads it with retry behavior.

`src/connectors/gdelt/export_parser.py`
Streams and parses GDELT export rows from ZIP files.

`src/ingestion/transform.py`
Normalizes raw rows, parses types, builds dedupe keys, and attaches category output.

`src/ingestion/repository.py`
Owns checkpoint persistence, ingestion run updates, bulk inserts, and stats queries.

`src/pipeline/ingest_service.py`
Main orchestration layer for ingestion, batching, metrics, and checkpoint flow.

`src/pipeline/data_quality.py`
Computes batch-level quality metrics during ingest.

`src/domain/events/categorization.py`
Deterministic category logic for root-code, cyber, and crisis classification.

`migrations/versions/`
Alembic migrations for stage 1 schema, stage 2 normalized schema, and stage 3 indexes.

`sql/`
Raw SQL versions of the schema and index stages.

`tests/`
Unit and integration coverage for categorization, transforms, repository logic, and CLI behavior.

## Testing

Run all tests:

```bash
pytest
```

Notes:

- unit tests run without Postgres
- integration tests use the database from `.env` unless `TEST_DATABASE_URL` is set

## Current State

The project is no longer just a print-to-console experiment. It already has:

- a working GDELT export connector
- PostgreSQL-backed ingestion state
- export checkpointing
- raw plus normalized event storage
- deterministic categorization
- CLI inspection commands for recent stored data

The next natural steps would be things like richer analytics, dashboards, alerting, better geo cleanup, and friendlier country-code presentation, but the current repo is already a usable ingestion and monitoring foundation.
