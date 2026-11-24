"""Small operational checks backed by the existing ingestion database."""

from datetime import datetime, timedelta, timezone

from src.db import fetch_all, fetch_one, get_connection
from src.ingestion.dashboard_queries import fetch_freshness
from src.ingestion.transform import SOURCE


def save_cycle(started_at: datetime, status: str) -> None:
    finished_at = None if status == "running" else datetime.now(timezone.utc)
    with get_connection() as connection:
        connection.execute("""
            INSERT INTO scheduler_health VALUES (true, %s, %s, %s)
            ON CONFLICT (singleton) DO UPDATE
            SET started_at = excluded.started_at, finished_at = excluded.finished_at, status = excluded.status
            WHERE scheduler_health.started_at <= excluded.started_at
        """, (started_at, finished_at, status))


def internal_health(connection, now: datetime) -> dict:
    scheduler = fetch_one(connection, "SELECT started_at, finished_at, status FROM scheduler_health")
    source = fetch_freshness(connection)
    last_export = source["latest_export_time_utc"]
    last_ingest = source["latest_ingestion_completed_at_utc"]

    lagged = now - timedelta(minutes=30)
    last_due = lagged.replace(minute=lagged.minute // 15 * 15, second=0, microsecond=0)
    slots = fetch_all(connection, """
        SELECT slot, coalesce(c.status, 'missing') AS status
        FROM generate_series(%s::timestamptz - interval '23 hours 45 minutes',
                             %s::timestamptz, interval '15 minutes') slot
        LEFT JOIN gdelt_export_checkpoints c ON c.source = %s AND c.export_time_utc = slot
        WHERE c.status IS DISTINCT FROM 'completed' ORDER BY slot DESC
    """, (last_due, last_due, SOURCE))
    issues = []
    if last_export is None or now - last_export > timedelta(hours=1):
        issues.append("source_stale")
    if last_ingest is None or now - last_ingest > timedelta(hours=1):
        issues.append("ingest_stale")
    if scheduler is None or now - scheduler["started_at"] > timedelta(minutes=25):
        issues.append("scheduler_stale")
    elif scheduler["status"] in {"failed", "timeout"}:
        issues.append(f"scheduler_{scheduler['status']}")
    if slots:
        issues.append("export_coverage_incomplete")
    return {
        "status": "degraded" if issues else "healthy",
        "issues": issues,
        "checked_at_utc": now,
        "latest_export_utc": last_export,
        "latest_successful_ingest_utc": last_ingest,
        "scheduler": scheduler,
        "coverage": {
            "expected": 96, "completed": 96 - len(slots),
            "missing": sum(row["status"] == "missing" for row in slots),
            "failed": sum(row["status"] == "failed" for row in slots),
            "latest_gaps": slots[:8],
        },
    }
