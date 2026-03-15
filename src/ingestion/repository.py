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
              AND processing_started_at < NOW() - %s::interval
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


def bulk_insert_events(
    connection: psycopg.Connection,
    events: Sequence[dict[str, Any]],
) -> int:
    """Insert normalized raw events, ignoring duplicates by dedupe key."""

    if not events:
        return 0

    rows = [
        (
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
        )
        for event in events
    ]

    with connection.cursor() as cursor:
        cursor.executemany(
            """
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
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            ON CONFLICT (source, dedupe_key) DO NOTHING
            """,
            rows,
        )
        return cursor.rowcount if cursor.rowcount is not None else 0
