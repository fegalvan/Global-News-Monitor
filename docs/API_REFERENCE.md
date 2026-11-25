# Global News Monitor API

The browser reads this FastAPI service through the same origin in production. Endpoint paths below are relative to FastAPI. The public site and local Vite proxy prefix them with `/api`.

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
  Legacy actor-pair counts for records with Goldstein scores at or below −5.

### Dashboard API contract

The API serves machine-coded GDELT event records. Treat CAMEO labels as
classification labels and source links as references to the reporting used by GDELT.

| Endpoint | Parameters | Response |
| --- | --- | --- |
| `GET /events` | Shared filters, `limit` (1–200, default 50), `before_time` and `before_id` | `filters`, `limit`, `rows`, `next_cursor` |
| `GET /analytics` | Shared filters, `bucket=hour\|day` (default `hour`), `include_map=false` to skip the legacy grid | `filters`, `bucket`, `summary`, `series`, `coverage`, `country_counts`, `country_counts_truncated`, `category_counts`, `map` |
| `GET /map` | Shared filters plus optional `south`, `west`, `north`, `east` | All matched coordinates in the viewport grouped into a bounded grid |
| `GET /countries` | None | Static catalogue of FIPS event geography codes |
| `GET /country-profile` | Shared filters with a required mapped `country`, `include_sources=false` to skip domain counts | Leading location names and, by default, source domains by record count |
| `GET /history` | Shared country and category filters and selected end | Coverage, matched comparisons, category changes, daily trend, and anomaly status |
| `GET /coverage` | Shared filters, optional source `domain`, `order=recent` (default) or `varied` | Counts, up to eight leading domains, and up to 12 original links with GDELT record context |
| `GET /health/monitor` | None | Scheduler heartbeat, freshness, and overdue export slots |
| `GET /freshness` | `max_age_minutes` (1–1440, default 60) | Source, ingestion, and event timestamps plus a status |
| `GET /spikes` | Shared filters optional, `hours` (1–720, default 24) | `rows` with resolved `filters`, or `hours` in the legacy form |
| `GET /tension` | Shared filters optional, `hours` (1–720, default 48) | `rows` with resolved `filters`, or `hours` in the legacy form |

The existing `/latest`, `/stats`, `/health`, and `/health/ready` routes retain
their response shapes.

**Shared filters.** Pass `country` as a stored two- or three-letter GDELT
country code, matched case-insensitively on input. GDELT event geography uses
two-character FIPS codes. The API still accepts three-character inputs for
compatibility with older callers. The country catalogue supplies the codes
used by current ingestion. Pass `category` as
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
  "event_code": "044", "original_event_code": "044", "event_label": "Meet at a third location",
  "actor1_name": null, "actor2_name": null,
  "country_code": "US", "country_name": "United States", "location_name": null,
  "latitude": null, "longitude": null,
  "primary_category": "diplomacy", "secondary_category": null,
  "category_confidence": 0.82, "goldstein_score": null,
  "source_url": null, "validation_flags": []
}
```

**Analytics.** Event counts use the same filtered rows. Buckets begin at UTC
hour or day boundaries. `coverage` reports expected and completed source slots
by export time and ignores country and category filters. The first and last
buckets may be partial. Missing values in `series` need the coverage check
described above before being displayed as zero. Country counts contain up to 250 codes,
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
  "coverage": [{"bucket_start": "2026-11-23T23:00:00+00:00", "expected_exports": 4, "completed_exports": 4}],
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

**News coverage.** `inspected_records` counts the latest 20,000 retained
records matching the filters at most, ordered by time and then event ID.
`truncated` indicates that another matching record exists, and
`earliest_inspected` gives the record time of the oldest inspected row.
`linked_records` counts inspected records with a usable URL. `unique_urls`
counts distinct trimmed stored URL strings. `domains` ranks up to eight
lowercased hostnames by unique links, with both `articles` and `records`
counts. `articles` lists up to 12 links. Each includes the original URL,
domain, record count, record time, GNM category, CAMEO code and label, actor names,
location name and precision, and `gdelt_id` when available. `order=varied`
applies source diversity and record detail to the newest 72 candidate URLs.
A `domain` narrows the link list and uses recent order. Totals and domain
counts describe the same inspected sample for either order. Historic links
only exist where the 17-day event detail is retained.

**Reporting history.** The comparison windows end on a complete UTC-hour
boundary. `absolute_change` is null when either window lacks a completed
source export. `percent_change` also requires at least 20 records in the
previous period. When querying all categories, `category_changes` gives
current and previous counts and the signed difference by category. It is
empty if either window is incomplete and omitted for a single category.
If archived records still use the former category rules, `category_history_from`
gives the first comparable hour. `category_rules_changed` marks affected
comparisons. The overall record totals remain comparable, while category
differences and category anomaly scores require consistent rules. The daily
category series leaves earlier values as gaps.
See the [walkthrough](TECHNICAL_WALKTHROUGH.md#compare-equal-periods-and-gate-the-score-on-coverage)
for the baseline, missing values, and anomaly thresholds.

**Legacy indicators.** The API retains `/spikes` and `/tension` for
existing clients. The dashboard uses `/history` for coverage-gated comparisons and
reporting anomaly detection. The legacy spikes endpoint groups by country name and category. Each candidate has
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
