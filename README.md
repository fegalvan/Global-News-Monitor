# Global News Monitor

Global News Monitor is a Python backend and data pipeline for ingesting structured GDELT event exports, normalizing and classifying the data, storing it in PostgreSQL, and exposing read APIs for downstream analytics and dashboards.

The project is designed as a production-aware ingestion service rather than a one-off script. It includes checkpointing, retry logic, idempotent inserts, single-run protection, dropped-row auditing, and readiness checks for API consumers.

## Project Overview

This system polls the latest GDELT Event export, parses the compressed dataset, transforms each row into a normalized event shape, applies deterministic category rules, and writes both raw and query-friendly records to PostgreSQL.

It also exposes:

- CLI commands for ingestion, migrations, and inspection
- a FastAPI read service for powering a dashboard
- operational metadata for ingestion runs, checkpoints, and rejected rows

## Features

- GDELT export ingestion over HTTPS
- Retry handling for transient network and HTTP failures
- Export-level checkpointing to avoid reprocessing the same file
- Deterministic deduplication using `GLOBALEVENTID` or a fallback hash
- Idempotent inserts with database uniqueness constraints
- PostgreSQL advisory locking so only one ingest runs at a time
- Raw and normalized event storage
- Dropped event audit trail for parse and validation failures
- FastAPI health and readiness endpoints
- CLI commands for recent events, ingestion runs, and aggregate stats

## Architecture

High-level flow:

```mermaid
flowchart LR
    A["Scheduler / CLI"] --> B["Ingest Service"]
    B --> C["Fetch latest export metadata"]
    C --> D["Download and parse GDELT export"]
    D --> E["Normalize + validate rows"]
    E --> F["Categorize events"]
    F --> G["raw_events"]
    F --> H["normalized_events"]
    E --> I["dropped_events"]
    G --> J["API / CLI queries"]
    H --> J
    B --> K["ingestion_runs + checkpoints"]
```

Core modules:

- `src/connectors/gdelt/`: fetch and parse GDELT exports
- `src/pipeline/ingest_service.py`: orchestration, metrics, and checkpoint flow
- `src/ingestion/transform.py`: normalization and dedupe key generation
- `src/ingestion/validation.py`: validation and cleanup rules
- `src/ingestion/repository.py`: database persistence and query helpers
- `src/api/main.py`: FastAPI read endpoints

## Setup Instructions

### Requirements

- Python 3.12+
- PostgreSQL

### Install dependencies

```bash
pip install -r requirements.txt
```

### Configure environment

Create a `.env` file at the repository root:

```env
DATABASE_URL=postgresql://username:password@localhost:5432/global_news_monitor
```

Optional:

```env
TEST_DATABASE_URL=postgresql://username:password@localhost:5432/global_news_monitor_test
INGEST_BATCH_SIZE=500
INGEST_READINESS_MAX_AGE_MINUTES=60
```

### Prepare the database

Run migrations:

```bash
python -m src.main migrate
```

## Usage

### Ingest the latest export

```bash
python -m src.main ingest
```

Typical first run behavior:

- discovers the latest export
- creates an ingestion run
- claims a checkpoint
- parses and inserts events
- marks the checkpoint as `completed`

Typical second run behavior for the same export:

- sees the checkpoint is already `completed`
- skips reprocessing
- does not insert duplicate rows

Example:

```text
checkpoint_status=completed outcome=already_completed
```

### Apply migrations

```bash
python -m src.main migrate
```

### Inspect recent normalized events

```bash
python -m src.main latest --limit 20
```

### Inspect recent ingestion runs

```bash
python -m src.main runs --limit 10
```

### Inspect aggregate stats

```bash
python -m src.main stats --hours 24
```

### Run the API locally

```bash
uvicorn src.api.main:app --reload
```

## API Endpoints

- `GET /health`
  Basic liveness check.

- `GET /health/ready`
  Readiness check that validates database connectivity and whether a recent successful ingest exists.

- `GET /latest`
  Recent normalized events.

- `GET /stats`
  Aggregate quality and category statistics.

- `GET /spikes`
  Category/country spike candidates.

- `GET /tension`
  High-tension actor interaction summaries.

## Database Design

Key tables:

- `ingestion_runs`
  Audit log for each ingest attempt, including status and row counters.

- `gdelt_export_checkpoints`
  Tracks export files by timestamp and status (`pending`, `processing`, `completed`, `failed`).

- `raw_events`
  Landing table with original payload, dedupe key, source metadata, and validation flags.

- `normalized_events`
  Query-friendly event table used by the API and reporting commands.

- `dropped_events`
  Audit table for parse failures and validation drops that were intentionally not inserted into the main event tables.

- `data_quality_audit`
  Lightweight quality snapshots captured during ingest.

## Idempotency + Reliability

Idempotency matters because this pipeline is intended to run repeatedly. If the same export is discovered twice, the system must not create duplicate data or corrupt downstream analytics.

This project handles that by combining:

- export checkpoints keyed by source and export timestamp
- unique constraints on checkpoint and event dedupe keys
- `ON CONFLICT DO NOTHING` inserts for raw events
- PostgreSQL advisory locking so only one ingest process runs at a time
- retry logic limited to transient failures such as connection errors, timeouts, HTTP `429`, and HTTP `5xx`

In practice, that means:

- a completed export is skipped on later runs
- duplicate raw events are not inserted again
- only one worker can actively ingest at a time
- invalid rows are visible in `dropped_events` instead of silently disappearing

## Testing

Run the full test suite:

```bash
pytest -q
```

The test suite covers:

- parsing and transformation helpers
- categorization logic
- repository behavior
- API responses
- retry classification
- advisory lock behavior
- readiness checks

## Future Improvements

- richer dashboard-oriented API endpoints and precomputed aggregates
- stronger data quality diagnostics for unknown country and event code handling
- structured metrics export for monitoring
- scheduled ingest runner for continuous polling
- frontend dashboard on top of the FastAPI service

## Why This Project Matters

This repository demonstrates backend and data engineering concerns that matter in real systems:

- reliable external data ingestion
- database-backed idempotency
- operational visibility into failures and dropped records
- clean separation between ingestion and read APIs
- practical production safeguards without unnecessary infrastructure
