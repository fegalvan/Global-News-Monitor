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
- PostgreSQL with PostGIS available on the server. The migration role must be able
  to enable the extension in the database, or an administrator can enable it first.
  On Windows, the PostgreSQL installer and Stack Builder provide both components.

Create a dedicated database, such as `global_news_monitor`, using your local
PostgreSQL tools. For integration tests, create a second database such as
`global_news_monitor_test`.

### Install dependencies

```bash
python -m venv .venv
```

Activate it with `source .venv/bin/activate` on macOS/Linux or
`.venv\Scripts\Activate.ps1` in PowerShell, then install dependencies.

```bash
python -m pip install -r requirements.txt
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

### Run continuous ingestion

```bash
python scripts/run_scheduler.py
```

Run the scheduler as a separate process from the API. It starts an ingest
immediately and targets a 15-minute interval between starts. It processes one
export at a time and continues polling after a failed or timed-out run.
It polls the latest export, so missed intervals are not automatically backfilled.

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

### Dashboard API contract

The API serves machine-coded GDELT event records. Treat CAMEO labels as
classification labels and source links as references to the reporting used by GDELT.

| Endpoint | Parameters | Response |
| --- | --- | --- |
| `GET /events` | Shared filters, `limit` (1–200, default 50), `before_time` and `before_id` | `filters`, `limit`, `rows`, `next_cursor` |
| `GET /analytics` | Shared filters, `bucket=hour\|day` (default `hour`) | `filters`, `bucket`, `summary`, `series`, `country_counts`, `country_counts_truncated`, `category_counts`, `map` |
| `GET /freshness` | `max_age_minutes` (1–1440, default 60) | Source, ingestion, and event timestamps plus a status |
| `GET /spikes` | Shared filters optional, `hours` (1–720, default 24) | `rows` with resolved `filters`, or `hours` in the legacy form |
| `GET /tension` | Shared filters optional, `hours` (1–720, default 48) | `rows` with resolved `filters`, or `hours` in the legacy form |

The existing `/latest`, `/stats`, `/health`, and `/health/ready` routes retain
their response shapes.

**Shared filters.** Pass `country` as a stored two- or three-letter GDELT
country code, matched case-insensitively on input. Code variants remain distinct
and the country counts supply the exact codes to send. Pass `category` as
`conflict`, `protest`, `politics`, `diplomacy`, `economics`, `cyber`, or
`crisis`. Use `start` (inclusive) and `end` (exclusive) as ISO 8601 timestamps
with a UTC offset. Windows span at most 30 days. The default end is the current
time and the default start is 24 hours before the end, or the `hours` window
specified for spikes and tension. An explicit start takes precedence over
`hours`. Use one explicit pair across all panels and pagination requests.
New response timestamps use UTC with `+00:00`. Occurrence dates use `YYYY-MM-DD`.

**Events.** Rows sort by `event_time_utc` descending, then `event_id` descending.
Send both values from `next_cursor` with the same filters to load the next page.
`next_cursor: null` marks the last page. The event timestamp comes from GDELT
`DATEADDED` when present. The existing normalization falls back to the
reported date at UTC midnight, then to the export time. The source occurrence
date is exposed separately in `event_date` and can be null. Coordinates are
decimal degrees with east-positive longitude. Both coordinates are null when
the record lacks a valid pair. These are the fields on an example row:

```json
{
  "event_id": 101, "global_event_id": null,
  "event_time_utc": "2026-11-23T23:45:00+00:00", "event_date": "2026-11-23",
  "event_code": "044", "original_event_code": "044", "event_label": "Meet",
  "actor1_name": null, "actor2_name": null,
  "country_code": "US", "country_name": "United States", "location_name": null,
  "latitude": null, "longitude": null,
  "primary_category": "diplomacy", "secondary_category": null,
  "category_confidence": 0.82, "goldstein_score": null,
  "source_url": null, "validation_flags": []
}
```

**Analytics.** All counts use the same filtered rows. Buckets begin at UTC
hour or day boundaries. Missing buckets represent zero events, and the first
and last buckets may be partial. Country counts contain up to 250 codes,
including a null-code group if present. The map returns the 500 busiest
two-degree cells as latitude and longitude centroids. `omitted_events` counts
valid locations outside those returned cells. The feed provides exact
coordinates for event detail.

```json
{
  "filters": {"start": "2026-11-23T23:00:00+00:00", "end": "2026-11-24T00:00:00+00:00", "country": "US", "category": null},
  "bucket": "hour",
  "summary": {"total_events": 2, "geolocated_events": 1, "countries_with_events": 1},
  "series": [{"bucket_start": "2026-11-23T23:00:00+00:00", "count": 2}],
  "country_counts": [{"country_code": "US", "country_name": "United States", "count": 2}],
  "country_counts_truncated": false,
  "category_counts": [{"primary_category": "diplomacy", "count": 2}],
  "map": {"cell_degrees": 2, "cells": [{"latitude": 39, "longitude": -77, "count": 1}], "omitted_events": 0}
}
```

**Freshness.** `latest_export_time_utc` is the greatest source timestamp on a
completed checkpoint. `latest_export_completed_at_utc` is when that same
checkpoint completed. `latest_ingestion_completed_at_utc` is the most recent
successful run that processed an export. Backfill can make the latter two
completion times differ. `latest_event_time_utc` is the latest normalized
event timestamp used by dashboard filters, and `latest_event_occurrence_date`
is the latest date reported in GDELT's `SQLDATE` field. These two maxima can
refer to different records. GDELT does not provide a time of day for `SQLDATE`.

```json
{
  "latest_export_time_utc": null, "latest_export_completed_at_utc": null,
  "latest_ingestion_completed_at_utc": null, "latest_event_time_utc": null,
  "latest_event_occurrence_date": null, "checked_at_utc": "2026-11-23T23:50:00+00:00",
  "max_age_minutes": 60, "status": "no_successful_ingest"
}
```

Statuses are `no_successful_ingest` when a completion or processed export is
missing, `ingestion_stale` when the last completion exceeds the threshold,
`source_stale` when completion is recent and the source export exceeds the
threshold, and `healthy` otherwise. Freshness reports these states with HTTP 200
when the database can be queried. The existing readiness route continues to
use ingestion completion age and returns HTTP 503 when it is unready.

**Indicators.** Spikes group by country name and category. Each candidate has
at least 10 events in the selected window and a matching baseline. Its
`recent_count` is compared with `baseline_avg` and population standard
deviation over UTC days with recorded events in the 15-day lookback before
the window's end, excluding the selected window. `lift_ratio` divides recent
count by the daily mean, and `z_score` is null when baseline variance is zero.
Missing days are excluded from the baseline. Windows other than 24 hours
compare different durations, and windows of 15 days or more exhaust the
lookback. Prefer a 24-hour window for interpretation. A short collection history
can produce no candidates. The endpoint returns at most 50 rows.

Tension groups actor pairs and category for events with a Goldstein score of
-5 or below, and requires at least three qualifying events per group. The
legacy `avg_tone` and `worst_tone` fields contain average and minimum
Goldstein scores. The endpoint returns at most 50 rows. Filtered responses
represent scores and ratios as JSON numbers when present. Both indicators
describe patterns in coded media coverage. Coverage gaps, repeated reporting,
and classification errors can affect the results.

**Empty and error states.** A valid empty query returns HTTP 200 with empty rows
or arrays and zero summary counts. Invalid parameters return HTTP 422 with a
`detail` field. Treat HTTP 5xx and network failures as unavailable data and
retain the last successful response if the dashboard already has one.

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
python -m pip install pytest
python -m pytest -q -rs
```

Set `TEST_DATABASE_URL` to a disposable PostGIS-enabled database to include
PostgreSQL integration tests. The suite applies the complete Alembic chain to
that database and skips database tests when the test URL is absent.

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
- frontend dashboard on top of the FastAPI service

## Why This Project Matters

This repository demonstrates backend and data engineering concerns that matter in real systems:

- reliable external data ingestion
- database-backed idempotency
- operational visibility into failures and dropped records
- clean separation between ingestion and read APIs
- practical production safeguards without unnecessary infrastructure
