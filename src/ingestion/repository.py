"""Repository functions for Stage 1 ingestion state and raw event persistence."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import psycopg


def insert_ingestion_run(
    connection: psycopg.Connection,
    run_id: UUID,
    trigger_mode: str,
    status: str = "started",
) -> None:
    """Create a new ingestion run audit record."""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ingestion_runs (
                id,
                status,
                trigger_mode
            )
            VALUES (%s, %s, %s)
            """,
            (run_id, status, trigger_mode),
        )


def update_ingestion_run(
    connection: psycopg.Connection,
    run_id: UUID,
    status: str,
    exports_seen: int | None = None,
    exports_completed: int | None = None,
    events_inserted: int | None = None,
    events_duplicated: int | None = None,
    error_summary: str | None = None,
    finished: bool = False,
) -> None:
    """Update ingestion run counters and final status."""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE ingestion_runs
            SET status = %s,
                exports_seen = COALESCE(%s, exports_seen),
                exports_completed = COALESCE(%s, exports_completed),
                events_inserted = COALESCE(%s, events_inserted),
                events_duplicated = COALESCE(%s, events_duplicated),
                error_summary = COALESCE(%s, error_summary),
                finished_at = CASE WHEN %s THEN NOW() ELSE finished_at END
            WHERE id = %s
            """,
            (
                status,
                exports_seen,
                exports_completed,
                events_inserted,
                events_duplicated,
                error_summary,
                finished,
                run_id,
            ),
        )


def insert_checkpoint(
    connection: psycopg.Connection,
    source: str,
    export_time_utc: datetime,
    export_url: str,
    export_filename: str,
    status: str = "pending",
) -> dict[str, Any]:
    """Insert a checkpoint if it does not already exist, then return the row."""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO gdelt_export_checkpoints (
                source,
                export_time_utc,
                export_url,
                export_filename,
                status
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source, export_time_utc) DO NOTHING
            """,
            (source, export_time_utc, export_url, export_filename, status),
        )

        cursor.execute(
            """
            SELECT *
            FROM gdelt_export_checkpoints
            WHERE source = %s AND export_time_utc = %s
            """,
            (source, export_time_utc),
        )
        checkpoint = cursor.fetchone()

    if checkpoint is None:
        raise RuntimeError("Failed to load checkpoint after insert.")

    return checkpoint


def claim_checkpoint(
    connection: psycopg.Connection,
    source: str,
    export_time_utc: datetime,
) -> dict[str, Any] | None:
    """Claim a checkpoint for processing if it is pending or failed."""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE gdelt_export_checkpoints
            SET status = 'processing',
                processing_started_at = NOW(),
                attempt_count = attempt_count + 1
            WHERE source = %s
              AND export_time_utc = %s
              AND status IN ('pending', 'failed')
            RETURNING *
            """,
            (source, export_time_utc),
        )
        return cursor.fetchone()


def reset_stale_processing_checkpoints(
    connection: psycopg.Connection,
    source: str,
    stale_after: timedelta,
) -> int:
    """Reset checkpoints stuck in processing beyond the stale threshold."""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE gdelt_export_checkpoints
            SET status = 'pending',
                processing_started_at = NULL,
                last_error = COALESCE(
                    last_error,
                    'Reset from processing to pending after stale timeout.'
                )
            WHERE source = %s
              AND status = 'processing'
              AND processing_started_at IS NOT NULL
              AND processing_started_at < NOW() - %s
            """,
            (source, stale_after),
        )
        return cursor.rowcount if cursor.rowcount is not None else 0


def mark_checkpoint_completed(
    connection: psycopg.Connection,
    source: str,
    export_time_utc: datetime,
    row_count_raw: int,
    row_count_inserted: int,
    row_count_duplicates: int,
) -> None:
    """Mark an export checkpoint as fully processed."""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE gdelt_export_checkpoints
            SET status = 'completed',
                completed_at = NOW(),
                last_error = NULL,
                row_count_raw = %s,
                row_count_inserted = %s,
                row_count_duplicates = %s
            WHERE source = %s AND export_time_utc = %s
            """,
            (
                row_count_raw,
                row_count_inserted,
                row_count_duplicates,
                source,
                export_time_utc,
            ),
        )


def mark_checkpoint_failed(
    connection: psycopg.Connection,
    source: str,
    export_time_utc: datetime,
    error_message: str,
) -> None:
    """Mark an export checkpoint as failed so it can be retried later."""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE gdelt_export_checkpoints
            SET status = 'failed',
                last_error = %s
            WHERE source = %s AND export_time_utc = %s
            """,
            (error_message, source, export_time_utc),
        )


def insert_raw_and_normalized_batch(
    connection: psycopg.Connection,
    events: Sequence[dict[str, Any]],
    batch_size: int = 500,
) -> tuple[int, int]:
    """Insert raw events and normalized rows in bulk batches."""

    if not events:
        return (0, 0)

    inserted_raw_count = 0
    inserted_normalized_count = 0
    batch_size = max(int(batch_size), 1)

    with connection.cursor() as cursor:
        for start in range(0, len(events), batch_size):
            chunk = events[start : start + batch_size]

            raw_placeholders = []
            raw_params: list[Any] = []
            for event in chunk:
                raw_placeholders.append(
                    "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)"
                )
                raw_params.extend(
                    [
                        event["source"],
                        event["export_time_utc"],
                        event["export_url"],
                        event["ingestion_run_id"],
                        event["dedupe_key"],
                        event.get("global_event_id"),
                        event.get("sql_date"),
                        event.get("event_time_utc"),
                        event.get("actor1_name"),
                        event.get("actor2_name"),
                        event.get("event_code"),
                        event.get("action_geo_full_name"),
                        event.get("action_geo_country_code"),
                        event.get("action_geo_lat"),
                        event.get("action_geo_long"),
                        event.get("avg_tone"),
                        json.dumps(event["raw_payload"]),
                    ]
                )

            # bulk insert raw rows and return identifiers for only the newly inserted rows
            raw_insert_query = f"""
                INSERT INTO raw_events (
                    source,
                    export_time_utc,
                    export_url,
                    ingestion_run_id,
                    dedupe_key,
                    global_event_id,
                    sql_date,
                    event_time_utc,
                    actor1_name,
                    actor2_name,
                    event_code,
                    action_geo_full_name,
                    action_geo_country_code,
                    action_geo_lat,
                    action_geo_long,
                    avg_tone,
                    raw_payload
                )
                VALUES {", ".join(raw_placeholders)}
                ON CONFLICT (source, dedupe_key) DO NOTHING
                RETURNING id, source, dedupe_key
            """
            cursor.execute(raw_insert_query, raw_params)
            inserted_rows = list(cursor.fetchall())
            inserted_raw_count += len(inserted_rows)

            if not inserted_rows:
                continue

            dedupe_to_raw_id = {
                (row["source"], row["dedupe_key"]): row["id"]
                for row in inserted_rows
            }

            normalized_placeholders = []
            normalized_params: list[Any] = []
            for event in chunk:
                raw_event_id = dedupe_to_raw_id.get((event["source"], event["dedupe_key"]))
                if raw_event_id is None:
                    continue

                normalized_placeholders.append(
                    "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                )
                normalized_params.extend(
                    [
                        raw_event_id,
                        event.get("event_time_utc"),
                        event.get("actor1_name"),
                        event.get("actor2_name"),
                        event.get("event_code"),
                        event.get("action_geo_country_code"),
                        event.get("action_geo_lat"),
                        event.get("action_geo_long"),
                        event.get("goldstein_score"),
                        event.get("primary_category"),
                        event.get("secondary_category"),
                        event.get("category_confidence"),
                        event.get("category_reason"),
                        event["source"],
                    ]
                )

            if not normalized_placeholders:
                continue

            normalized_insert_query = f"""
                INSERT INTO normalized_events (
                    raw_event_id,
                    event_time_utc,
                    actor1_name,
                    actor2_name,
                    event_code,
                    country_code,
                    latitude,
                    longitude,
                    goldstein_score,
                    primary_category,
                    secondary_category,
                    category_confidence,
                    category_reason,
                    source
                )
                VALUES {", ".join(normalized_placeholders)}
                ON CONFLICT (raw_event_id) DO NOTHING
            """
            cursor.execute(normalized_insert_query, normalized_params)
            inserted_normalized_count += cursor.rowcount if cursor.rowcount is not None else 0

    return (inserted_raw_count, inserted_normalized_count)


def fetch_recent_normalized_events(
    connection: psycopg.Connection,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Fetch the most recent normalized events for CLI inspection."""

    safe_limit = max(int(limit), 1)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                event_time_utc,
                actor1_name,
                actor2_name,
                event_code,
                country_code,
                latitude,
                longitude,
                primary_category,
                secondary_category,
                category_confidence,
                goldstein_score
            FROM normalized_events
            ORDER BY event_time_utc DESC NULLS LAST, event_id DESC
            LIMIT %s
            """,
            (safe_limit,),
        )
        return list(cursor.fetchall())


def fetch_recent_ingestion_runs(
    connection: psycopg.Connection,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Fetch the latest ingestion run records for CLI inspection."""

    safe_limit = max(int(limit), 1)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                id,
                started_at,
                finished_at,
                status,
                trigger_mode,
                exports_seen,
                exports_completed,
                events_inserted,
                events_duplicated,
                error_summary
            FROM ingestion_runs
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (safe_limit,),
        )
        return list(cursor.fetchall())


def fetch_event_stats(
    connection: psycopg.Connection,
    hours: int = 24,
) -> dict[str, Any]:
    """Fetch aggregate event stats for a recent time window."""

    safe_hours = max(int(hours), 1)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            WITH windowed AS (
                SELECT *
                FROM normalized_events
                WHERE event_time_utc >= NOW() - make_interval(hours => %s)
            ),
            normalized_windowed AS (
                SELECT
                    *,
                    CASE
                        WHEN country_code IS NULL THEN NULL
                        WHEN UPPER(BTRIM(country_code)) IN ('', 'UNKNOWN', 'NULL', 'NONE', 'N/A', 'NA', '-') THEN NULL
                        WHEN UPPER(BTRIM(country_code)) ~ '^[A-Z]{2,3}$' THEN UPPER(BTRIM(country_code))
                        ELSE NULL
                    END AS country_code_norm
                FROM windowed
            )
            SELECT
                COUNT(*) AS total_events,
                SUM(
                    CASE
                        WHEN (COALESCE(TRIM(actor1_name), '') = '')
                         AND (COALESCE(TRIM(actor2_name), '') = '')
                        THEN 1
                        ELSE 0
                    END
                ) AS missing_actor_count,
                SUM(
                    CASE
                        WHEN country_code_norm IS NULL
                         AND latitude IS NULL
                         AND longitude IS NULL
                        THEN 1
                        ELSE 0
                    END
                ) AS missing_geo_count,
                SUM(
                    CASE
                        WHEN category_reason = 'fallback_unknown_event_code'
                        THEN 1
                        ELSE 0
                    END
                ) AS fallback_unknown_category_count
            FROM normalized_windowed
            """,
            (safe_hours,),
        )
        overview = cursor.fetchone() or {}

        cursor.execute(
            """
            SELECT primary_category, COUNT(*) AS count
            FROM normalized_events
            WHERE event_time_utc >= NOW() - make_interval(hours => %s)
            GROUP BY primary_category
            ORDER BY count DESC, primary_category ASC
            """,
            (safe_hours,),
        )
        category_counts = list(cursor.fetchall())

        cursor.execute(
            """
            WITH normalized_country AS (
                SELECT
                    CASE
                        WHEN country_code IS NULL THEN NULL
                        WHEN UPPER(BTRIM(country_code)) IN ('', 'UNKNOWN', 'NULL', 'NONE', 'N/A', 'NA', '-') THEN NULL
                        WHEN UPPER(BTRIM(country_code)) ~ '^[A-Z]{2,3}$' THEN UPPER(BTRIM(country_code))
                        ELSE NULL
                    END AS country_code
                FROM normalized_events
                WHERE event_time_utc >= NOW() - make_interval(hours => %s)
            )
            SELECT country_code, COUNT(*) AS count
            FROM normalized_country
            GROUP BY country_code
            ORDER BY count DESC NULLS LAST, country_code ASC NULLS LAST
            LIMIT 10
            """,
            (safe_hours,),
        )
        top_countries = list(cursor.fetchall())

        cursor.execute(
            """
            SELECT event_code, COUNT(*) AS count
            FROM normalized_events
            WHERE event_time_utc >= NOW() - make_interval(hours => %s)
            GROUP BY event_code
            ORDER BY count DESC NULLS LAST, event_code ASC NULLS LAST
            """,
            (safe_hours,),
        )
        event_code_counts = list(cursor.fetchall())

    return {
        "hours": safe_hours,
        "overview": overview,
        "category_counts": category_counts,
        "top_countries": top_countries,
        "event_code_counts": event_code_counts,
    }
