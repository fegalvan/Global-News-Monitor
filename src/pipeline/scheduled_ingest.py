"""Discover and process a bounded set of recent GDELT exports."""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from zipfile import BadZipFile

from psycopg import Connection
from dotenv import load_dotenv
from requests import HTTPError

from src.connectors.gdelt import get_latest_export_metadata
from src.connectors.gdelt.export_client import GCS_GDELT_PREFIX
from src.db import fetch_all, get_connection, transaction
from src.ingestion.repository import release_ingestion_lock, try_acquire_ingestion_lock
from src.ingestion.transform import SOURCE
from src.pipeline.ingest_service import STALE_CHECKPOINT_AFTER, ingest_export

logger = logging.getLogger(__name__)
EXPORT_INTERVAL = timedelta(minutes=15)
ARCHIVE = f"{GCS_GDELT_PREFIX}/gdeltv2/"


def bounded_env(name: str, default: int, minimum: int, maximum: int) -> int:
    value = int(os.getenv(name, str(default)))
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def select_exports(
    latest: datetime,
    lookback: timedelta,
    limit: int,
    checkpoints: dict[datetime, dict[str, Any]],
    now: datetime,
) -> list[datetime]:
    eligible = []
    for offset in range(int(lookback / EXPORT_INTERVAL), -1, -1):
        slot = latest - offset * EXPORT_INTERVAL
        checkpoint = checkpoints.get(slot)
        if checkpoint:
            status = checkpoint["status"]
            started_at = checkpoint["processing_started_at"]
            if status in {"completed", "expired"}:
                continue
            if started_at and status == "processing" and now - started_at <= STALE_CHECKPOINT_AFTER:
                continue
            if started_at and status == "failed":
                delay = EXPORT_INTERVAL * 2 ** min(max(checkpoint["attempt_count"] - 1, 0), 4)
                if now - started_at < delay:
                    continue
        eligible.append(slot)
    if len(eligible) > limit and eligible[-1] == latest:
        return [*eligible[:limit - 1], latest]
    return eligible[:limit]


def prune_history(connection: Connection, keep_days: int, *, limit: int = 10_000) -> int:
    """Expire old export coverage and delete a bounded number of detail rows."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)
    with transaction(connection):
        connection.execute(
            """
            UPDATE gdelt_export_checkpoints SET status = 'expired'
            WHERE source = %s AND status = 'completed' AND export_time_utc < %s
            """,
            (SOURCE, cutoff),
        )
        ids = [
            row["id"] for row in fetch_all(
                connection,
                "SELECT id FROM raw_events WHERE export_time_utc < %s ORDER BY export_time_utc LIMIT %s",
                (cutoff, limit),
            )
        ]
        if ids:
            connection.execute("DELETE FROM normalized_events WHERE raw_event_id = ANY(%s)", (ids,))
            connection.execute("DELETE FROM raw_events WHERE id = ANY(%s)", (ids,))
        connection.execute(
            """
            DELETE FROM dropped_events WHERE id IN
                (SELECT id FROM dropped_events WHERE export_time_utc < %s ORDER BY id LIMIT %s)
            """,
            (cutoff, limit),
        )
    return len(ids)


def sync_recent_exports() -> None:
    load_dotenv()
    lookback = timedelta(hours=bounded_env("GDELT_LOOKBACK_HOURS", 24, 0, 168))
    limit = bounded_env("GDELT_MAX_EXPORTS_PER_RUN", 8, 2, 24)
    keep_days = bounded_env("GDELT_RETENTION_DAYS", 0, 0, 90)
    if keep_days and timedelta(days=keep_days) <= lookback:
        raise ValueError("Enabled retention must exceed the catch-up lookback")

    with get_connection() as connection:
        if not try_acquire_ingestion_lock(connection):
            logger.info("scheduled_ingestion_skipped reason=lock_held")
            return
        try:
            metadata = get_latest_export_metadata()
            latest = metadata["export_time_utc"]
            export_url = metadata["export_url"]
            if lookback and (
                export_url != f"{ARCHIVE}{latest:%Y%m%d%H%M%S}.export.CSV.zip"
                or latest.minute % 15 or latest.second
            ):
                raise ValueError("Catch-up requires an aligned export from the official GDELT archive")

            rows = fetch_all(
                connection,
                """
                SELECT export_time_utc, status, processing_started_at, attempt_count
                FROM gdelt_export_checkpoints
                WHERE source = %s AND export_time_utc BETWEEN %s AND %s
                """,
                (SOURCE, latest - lookback, latest),
            )
            checkpoints = {row["export_time_utc"]: row for row in rows}
            now = datetime.now(timezone.utc)
            slots = select_exports(latest, lookback, limit, checkpoints, now)
            if keep_days:
                slots = [slot for slot in slots if slot >= now - timedelta(days=keep_days)]
            logger.info("scheduled_ingestion_plan latest=%s exports=%s", latest.isoformat(), len(slots))
            for slot in slots:
                url = export_url if slot == latest else f"{ARCHIVE}{slot:%Y%m%d%H%M%S}.export.CSV.zip"
                try:
                    ingest_export(url, connection=connection, trigger_mode="scheduled")
                except HTTPError as exc:
                    if exc.response is None or exc.response.status_code not in {404, 410}:
                        raise
                    logger.warning("scheduled_export_unavailable timestamp=%s", slot.isoformat())
                except (BadZipFile, UnicodeError):
                    logger.warning("scheduled_export_unreadable timestamp=%s", slot.isoformat())

            if keep_days:
                removed = prune_history(connection, keep_days)
                if removed:
                    logger.info("scheduled_retention_pruned raw_records=%s", removed)
        finally:
            if not connection.closed:
                release_ingestion_lock(connection)
