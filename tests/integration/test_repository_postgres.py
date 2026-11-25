from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4
from zipfile import BadZipFile
import os

import psycopg
import pytest
from requests import ConnectionError, HTTPError, Response
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from fastapi.testclient import TestClient

from src.api.main import app
try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None

from src.ingestion.repository import (
    claim_checkpoint,
    fetch_latest_successful_ingestion,
    insert_dropped_events,
    insert_data_quality_audit,
    insert_checkpoint,
    insert_ingestion_run,
    insert_raw_and_normalized_batch,
    mark_checkpoint_completed,
    release_ingestion_lock,
    try_acquire_ingestion_lock,
    update_ingestion_run,
)
from src.ingestion.validation import validate_and_clean_event
from src.pipeline import ingest_service, scheduled_ingest


@pytest.fixture(scope="session")
def pg_connection():
    root = Path(__file__).resolve().parents[2]
    if load_dotenv is not None:
        load_dotenv(root / ".env")

    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Set TEST_DATABASE_URL to a dedicated Postgres integration database.")

    config = Config(str(root / "alembic.ini"))
    config.attributes["database_url"] = database_url
    command.upgrade(config, "head")

    with psycopg.connect(database_url, row_factory=psycopg.rows.dict_row, autocommit=True) as connection:
        yield connection


@pytest.fixture(autouse=True)
def clean_tables(pg_connection):
    with pg_connection.transaction(force_rollback=True):
        with pg_connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE dropped_events, data_quality_audit, normalized_events, raw_events, "
                "gdelt_export_checkpoints, ingestion_runs, event_hourly_counts, event_history_coverage, "
                "scheduler_health RESTART IDENTITY CASCADE"
            )
        yield


def test_migrations_reach_head(pg_connection):
    with pg_connection.cursor() as cursor:
        cursor.execute("SELECT version_num FROM alembic_version")
        assert cursor.fetchone()["version_num"] == ScriptDirectory.from_config(Config("alembic.ini")).get_current_head()
        cursor.execute(
            """
            SELECT character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'alembic_version'
              AND column_name = 'version_num'
            """
        )
        length = cursor.fetchone()["character_maximum_length"]
        assert length is None or length >= len("0005_country_mapping_and_quality_audit")


def test_latest_successful_ingestion_ignores_skipped_exports(pg_connection):
    assert fetch_latest_successful_ingestion(pg_connection) is None

    successful_run_id = uuid4()
    skipped_run_id = uuid4()
    for run_id, exports_completed in ((successful_run_id, 1), (skipped_run_id, 0)):
        insert_ingestion_run(pg_connection, run_id=run_id, trigger_mode="manual", status="started")
        update_ingestion_run(
            pg_connection,
            run_id=run_id,
            status="completed",
            exports_seen=1,
            exports_completed=exports_completed,
            finished=True,
        )

    pg_connection.execute(
        "UPDATE ingestion_runs SET finished_at = NOW() - INTERVAL '1 hour' WHERE id = %s",
        (successful_run_id,),
    )

    assert fetch_latest_successful_ingestion(pg_connection)["id"] == successful_run_id


def _build_event(run_id, dedupe_key: str) -> dict:
    return {
        "source": "gdelt_events_v2",
        "export_time_utc": datetime(2026, 3, 15, 0, 15, tzinfo=timezone.utc),
        "export_url": "https://data.gdeltproject.org/gdeltv2/20260315001500.export.CSV.zip",
        "ingestion_run_id": run_id,
        "dedupe_key": dedupe_key,
        "global_event_id": 100,
        "sql_date": datetime(2026, 3, 15, tzinfo=timezone.utc).date(),
        "event_time_utc": datetime(2026, 3, 15, 0, 0, tzinfo=timezone.utc),
        "actor1_name": "POLICE",
        "actor2_name": "PROTESTERS",
        "event_code": "190",
        "action_geo_full_name": "Washington, DC",
        "action_geo_country_code": "USA",
        "country_name": "United States",
        "action_geo_lat": Decimal("38.9072"),
        "action_geo_long": Decimal("-77.0369"),
        "avg_tone": Decimal("-2.5"),
        "goldstein_score": Decimal("-5.0"),
        "primary_category": "conflict",
        "secondary_category": None,
        "category_confidence": Decimal("0.82"),
        "category_reason": "root_event_code:19",
        "raw_payload": {"EventCode": "190"},
    }


@pytest.fixture
def dashboard_client(pg_connection, monkeypatch):
    for module in ("src.api.dashboard", "src.api.main", "src.main"):
        monkeypatch.setattr(f"{module}.get_connection", lambda: nullcontext(pg_connection))
    with TestClient(app) as client:
        yield client


@pytest.fixture
def dashboard_window(pg_connection):
    start = datetime(2026, 3, 15, tzinfo=timezone.utc)
    end = start + timedelta(hours=2)
    run_id = uuid4()
    insert_ingestion_run(pg_connection, run_id=run_id, trigger_mode="manual", status="started")
    common = _build_event(run_id, "unused")
    rows = [
        dict(common, dedupe_key="before", event_time_utc=start - timedelta(microseconds=1)),
        dict(common, dedupe_key="start", event_time_utc=start),
        dict(common, dedupe_key="tie-one", event_time_utc=start, action_geo_long=None),
        dict(common, dedupe_key="tie-two", event_time_utc=start),
        dict(common, dedupe_key="other-category", primary_category="politics", event_time_utc=start),
        dict(common, dedupe_key="other-country", action_geo_country_code="CA", country_name="Canada",
             action_geo_lat=90, action_geo_long=180, event_time_utc=start + timedelta(hours=1),
             raw_payload={"EventCode": "190", "ActionGeo_CountryCode": "CA", "Actor1CountryCode": "GBR",
                          "ActionGeo_Type": "4", "SOURCEURL": "https://Report.Example:443/article"}),
        dict(common, dedupe_key="end", event_time_utc=end),
    ]
    insert_raw_and_normalized_batch(pg_connection, rows)
    # Use a non-UTC session to exercise UTC output and bucket semantics.
    pg_connection.execute("SET LOCAL TIME ZONE 'America/New_York'")
    return {"start": start.isoformat(), "end": end.isoformat()}


def test_dashboard_events_filter_and_paginate(dashboard_client, dashboard_window):
    filters = dict(dashboard_window, country="usa", category="conflict", limit=2)
    first = dashboard_client.get("/events", params=filters)
    assert first.status_code == 200, first.text
    first = first.json()
    assert first["filters"]["country"] == "USA"
    assert len(first["rows"]) == 2
    assert first["rows"][0]["event_label"] == "Fight"
    assert first["rows"][0]["event_code"] == "190"
    assert first["rows"][0]["original_event_code"] == "190"
    assert first["rows"][0]["location_name"] == "Washington, DC"
    assert first["rows"][0]["actor1_name"] == "POLICE"
    assert first["rows"][0]["goldstein_score"] == -5
    assert first["rows"][0]["category_confidence"] == 0.82
    assert first["rows"][1]["latitude"] is None and first["rows"][1]["longitude"] is None
    assert first["rows"][0]["event_time_utc"] == dashboard_window["start"]

    second = dashboard_client.get("/events", params={**filters, **first["next_cursor"]}).json()
    assert len(second["rows"]) == 1 and second["next_cursor"] is None
    ids = [row["event_id"] for row in first["rows"] + second["rows"]]
    assert ids == sorted(set(ids), reverse=True)

    empty = dashboard_client.get("/events", params={**filters, "country": "GB"}).json()
    assert empty["rows"] == [] and empty["next_cursor"] is None


@pytest.mark.parametrize(
    "path, params",
    [
        ("/events", {"limit": 0}),
        ("/events", {"limit": 201}),
        ("/events", {"before_id": 1}),
        ("/events", {"country": "United States"}),
        ("/events", {"category": "other"}),
        ("/events", {"start": "2026-03-15T00:00:00"}),
        ("/events", {"start": "1773532800"}),
        ("/analytics", {"start": "2026-03-15"}),
        ("/analytics", {"start": "2026-04-15T00:00:00Z"}),
        ("/analytics", {"start": "2026-02-01T00:00:00Z"}),
        ("/analytics", {"bucket": "week"}),
    ],
)
def test_dashboard_rejects_invalid_queries(path, params):
    with TestClient(app) as client:
        response = client.get(
            path, params={"start": "2026-03-15T00:00:00Z", "end": "2026-03-15T02:00:00Z", **params},
        )
    assert response.status_code == 422, response.text


def test_dashboard_analytics_and_filtered_indicators(dashboard_client, dashboard_window):
    response = dashboard_client.get("/analytics", params=dashboard_window)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["summary"] == {"total_events": 5, "geolocated_events": 4, "countries_with_events": 2}
    assert [row["count"] for row in body["series"]] == [4, 1]
    assert body["series"][0]["bucket_start"] == dashboard_window["start"]
    assert [row["count"] for row in body["country_counts"]] == [4, 1]
    assert body["category_counts"] == [
        {"primary_category": "conflict", "count": 4}, {"primary_category": "politics", "count": 1},
    ]
    assert body["map"]["cells"] == [
        {"latitude": 39, "longitude": -77, "count": 3},
        {"latitude": 89, "longitude": 179, "count": 1},
    ]

    filtered = dict(dashboard_window, country="USA", category="conflict")
    daily = dashboard_client.get("/analytics", params={**filtered, "bucket": "day"}).json()
    assert daily["summary"]["total_events"] == 3
    assert daily["series"] == [{"bucket_start": dashboard_window["start"], "count": 3}]
    assert daily["map"]["cells"][0]["count"] == 2
    assert dashboard_client.get("/spikes", params=filtered).json()["rows"] == []
    tension = dashboard_client.get("/tension", params=filtered).json()
    assert tension["rows"][0]["event_count"] == 3
    assert tension["rows"][0]["avg_tone"] == -5
    empty = dashboard_client.get("/analytics", params={**filtered, "country": "GB"}).json()
    assert empty["summary"]["total_events"] == 0
    assert empty["series"] == empty["country_counts"] == empty["category_counts"] == empty["map"]["cells"] == []


def test_dashboard_excludes_invalid_source_coordinates(dashboard_client, pg_connection, dashboard_window):
    run_id = pg_connection.execute("SELECT id FROM ingestion_runs").fetchone()["id"]
    event = _build_event(run_id, "invalid-geo")
    event.update(primary_category="crisis", action_geo_long=Decimal("181"))
    cleaned, flags, drop = validate_and_clean_event(event, event["export_time_utc"])
    assert not drop
    cleaned["validation_flags"] = flags
    insert_raw_and_normalized_batch(pg_connection, [cleaned])

    params = dict(dashboard_window, category="crisis")
    event_row, = dashboard_client.get("/events", params=params).json()["rows"]
    assert event_row["latitude"] is None and event_row["longitude"] is None
    assert "invalid_geo_range" in event_row["validation_flags"]
    analytics = dashboard_client.get("/analytics", params=params).json()
    assert analytics["summary"]["total_events"] == 1
    assert analytics["summary"]["geolocated_events"] == 0
    assert analytics["map"]["cells"] == []


def test_dashboard_filtered_spikes_keep_existing_baseline_math(dashboard_client, pg_connection, dashboard_window):
    start = datetime.fromisoformat(dashboard_window["start"])
    run_id = pg_connection.execute("SELECT id FROM ingestion_runs").fetchone()["id"]
    common = _build_event(run_id, "unused")
    rows = [
        dict(common, dedupe_key=f"spike-{days_before}-{index}", primary_category="economics",
             event_time_utc=start - timedelta(days=days_before))
        for days_before, count in ((0, 10), (1, 3), (2, 1))
        for index in range(count)
    ]
    insert_raw_and_normalized_batch(pg_connection, rows)
    params = dict(dashboard_window, country="USA", category="economics")
    response = dashboard_client.get("/spikes", params=params)
    assert response.status_code == 200, response.text
    assert response.json()["rows"] == [{
        "category": "economics", "country_name": "United States", "recent_count": 10,
        "baseline_avg": 2, "z_score": 8, "lift_ratio": 5,
    }]


def test_dashboard_freshness_distinguishes_source_and_completion(dashboard_client, pg_connection, dashboard_window):
    empty = dashboard_client.get("/freshness").json()
    assert empty["status"] == "no_successful_ingest"
    assert empty["latest_export_time_utc"] is None
    assert empty["latest_ingestion_completed_at_utc"] is None
    assert dashboard_client.get("/health/ready").status_code == 503

    source_time = datetime.now(timezone.utc) - timedelta(hours=2)
    insert_checkpoint(
        pg_connection, source="gdelt_events_v2", export_time_utc=source_time,
        export_url="https://example.invalid/export", export_filename="export",
    )
    mark_checkpoint_completed(
        pg_connection, source="gdelt_events_v2", export_time_utc=source_time,
        row_count_raw=6, row_count_inserted=6, row_count_duplicates=0,
    )
    run_id = pg_connection.execute("SELECT id FROM ingestion_runs").fetchone()["id"]
    update_ingestion_run(pg_connection, run_id=run_id, status="completed", exports_completed=1, finished=True)
    body = dashboard_client.get("/freshness").json()
    assert body["status"] == "source_stale"
    assert datetime.fromisoformat(body["latest_export_time_utc"]) == source_time
    assert body["latest_export_completed_at_utc"] == body["latest_ingestion_completed_at_utc"]
    assert body["latest_event_time_utc"] == dashboard_window["end"]
    assert body["latest_event_occurrence_date"] == "2026-03-15"
    assert dashboard_client.get("/freshness", params={"max_age_minutes": 180}).json()["status"] == "healthy"
    assert dashboard_client.get("/health/ready").status_code == 200
    pg_connection.execute("UPDATE ingestion_runs SET finished_at = NOW() - INTERVAL '2 hours'")
    assert dashboard_client.get("/freshness").json()["status"] == "ingestion_stale"
    assert dashboard_client.get("/health/ready").status_code == 503


def test_dashboard_coverage_distinguishes_completed_failed_and_expired(dashboard_client, pg_connection):
    start = datetime(2026, 3, 15, tzinfo=timezone.utc)
    for offset, status in ((0, "completed"), (15, "failed"), (30, "expired"), (60, "completed")):
        slot = start + timedelta(minutes=offset)
        insert_checkpoint(
            pg_connection, source="gdelt_events_v2", export_time_utc=slot,
            export_url="https://example.invalid/export", export_filename="export", status=status,
        )
    response = dashboard_client.get("/analytics", params={
        "start": start.isoformat(), "end": (start + timedelta(hours=1)).isoformat(),
        "country": "CA",
    })
    assert response.status_code == 200, response.text
    assert response.json()["coverage"] == [{
        "bucket_start": start.isoformat(), "expected_exports": 4, "completed_exports": 1,
    }]


def test_scheduled_sync_prioritizes_freshness_and_retries_partial_exports(pg_connection, monkeypatch):
    now = datetime.now(timezone.utc)
    latest = now.replace(minute=now.minute // 15 * 15, second=0, microsecond=0)
    oldest = latest - timedelta(hours=1)
    calls = []
    first_attempt = True

    def url(slot):
        return f"{scheduled_ingest.ARCHIVE}{slot:%Y%m%d%H%M%S}.export.CSV.zip"

    def rows(export_url, **_):
        nonlocal first_attempt
        slot = ingest_service.parse_export_metadata(export_url)["export_time_utc"]
        calls.append(slot)
        if slot == oldest and first_attempt:
            response = Response()
            response.status_code = 404
            raise HTTPError(response=response)
        yield {"GLOBALEVENTID": str(int(slot.timestamp())), "EventCode": "190"}
        if slot == latest and first_attempt:
            first_attempt = False
            raise ConnectionError("interrupted after the first committed chunk")

    monkeypatch.setenv("GDELT_LOOKBACK_HOURS", "1")
    monkeypatch.setenv("GDELT_MAX_EXPORTS_PER_RUN", "2")
    monkeypatch.setenv("GDELT_RETENTION_DAYS", "0")
    monkeypatch.setattr(scheduled_ingest, "get_connection", lambda: nullcontext(pg_connection))
    monkeypatch.setattr(scheduled_ingest, "get_latest_export_metadata", lambda: {
        "export_time_utc": latest, "export_url": url(latest),
    })
    monkeypatch.setattr(ingest_service, "BATCH_SIZE", 1)
    monkeypatch.setattr(ingest_service, "iter_export_rows", rows)

    with pytest.raises(ConnectionError):
        scheduled_ingest.sync_recent_exports()
    assert calls == [oldest, latest]

    scheduled_ingest.sync_recent_exports()
    assert calls[-2:] == [oldest + timedelta(minutes=15), oldest + timedelta(minutes=30)]
    pg_connection.execute("""
        UPDATE gdelt_export_checkpoints
        SET processing_started_at = NOW() - INTERVAL '1 hour' WHERE status = 'failed'
    """)
    scheduled_ingest.sync_recent_exports()
    assert calls[-2:] == [oldest, latest]
    scheduled_ingest.sync_recent_exports()
    assert calls[-1] == latest - timedelta(minutes=15)
    scheduled_ingest.sync_recent_exports()
    assert len(calls) == 7

    checkpoints = pg_connection.execute("""
        SELECT status, attempt_count, row_count_duplicates FROM gdelt_export_checkpoints
        ORDER BY export_time_utc
    """).fetchall()
    assert [row["status"] for row in checkpoints] == ["completed"] * 5
    assert checkpoints[-1]["attempt_count"] == 2 and checkpoints[-1]["row_count_duplicates"] == 1
    for table in ("raw_events", "normalized_events"):
        assert pg_connection.execute(
            psycopg.sql.SQL("SELECT count(*) AS count FROM {}").format(psycopg.sql.Identifier(table))
        ).fetchone()["count"] == 5


def test_scheduled_sync_skips_when_another_session_holds_lock(pg_connection, monkeypatch):
    monkeypatch.setattr(scheduled_ingest, "get_connection", lambda: nullcontext(pg_connection))
    with psycopg.connect(os.environ["TEST_DATABASE_URL"], row_factory=psycopg.rows.dict_row) as other:
        assert try_acquire_ingestion_lock(other)
        monkeypatch.setattr(scheduled_ingest, "get_latest_export_metadata", lambda: pytest.fail("lock was bypassed"))
        scheduled_ingest.sync_recent_exports()


def test_fully_unparseable_export_is_failed_and_never_counted_as_coverage(pg_connection, monkeypatch):
    def unreadable_rows(_url, *, on_parse_error):
        on_parse_error("parse_error", {"row_number": 1, "row_length": 1})
        return iter(())

    monkeypatch.setattr(ingest_service, "iter_export_rows", unreadable_rows)
    url = f"{scheduled_ingest.ARCHIVE}20260315000000.export.CSV.zip"
    with pytest.raises(BadZipFile, match="only unparseable"):
        ingest_service.ingest_export(url, connection=pg_connection)
    assert pg_connection.execute("SELECT status FROM gdelt_export_checkpoints").fetchone()["status"] == "failed"
    assert pg_connection.execute("SELECT count(*) AS count FROM dropped_events").fetchone()["count"] == 1


def test_retention_expires_coverage_and_prunes_detail_in_bounded_batches(pg_connection):
    run_id = uuid4()
    insert_ingestion_run(pg_connection, run_id=run_id, trigger_mode="manual", status="started")
    recent = datetime.now(timezone.utc)
    old = recent - timedelta(days=20)
    rows = [dict(_build_event(run_id, key), export_time_utc=slot)
            for key, slot in (("old-one", old), ("old-two", old), ("recent", recent))]
    insert_raw_and_normalized_batch(pg_connection, rows)
    for slot in (old, recent):
        insert_checkpoint(pg_connection, "gdelt_events_v2", slot, "https://example.invalid/export", "export", "completed")
    assert scheduled_ingest.prune_history(pg_connection, 17, limit=1) == 1
    assert scheduled_ingest.prune_history(pg_connection, 17, limit=1) == 1
    assert scheduled_ingest.prune_history(pg_connection, 17, limit=1) == 0
    assert pg_connection.execute("SELECT count(*) AS count FROM normalized_events").fetchone()["count"] == 1
    assert pg_connection.execute("SELECT dedupe_key FROM raw_events").fetchone()["dedupe_key"] == "recent"
    states = pg_connection.execute("SELECT status FROM gdelt_export_checkpoints ORDER BY export_time_utc").fetchall()
    assert states == [{"status": "expired"}, {"status": "completed"}]
    counts = pg_connection.execute("SELECT live_count, historical_count FROM event_hourly_counts").fetchone()
    assert counts == {"live_count": 1, "historical_count": 3}



def test_ingestion_run_and_checkpoint_creation(pg_connection):
    run_id = uuid4()
    export_time = datetime(2026, 3, 15, 0, 15, tzinfo=timezone.utc)
    insert_ingestion_run(pg_connection, run_id=run_id, trigger_mode="manual", status="started")
    checkpoint = insert_checkpoint(
        pg_connection,
        source="gdelt_events_v2",
        export_time_utc=export_time,
        export_url="https://data.gdeltproject.org/gdeltv2/20260315001500.export.CSV.zip",
        export_filename="20260315001500.export.CSV.zip",
    )
    claimed = claim_checkpoint(pg_connection, source="gdelt_events_v2", export_time_utc=export_time)

    assert checkpoint["status"] == "pending"
    assert claimed is not None
    assert claimed["status"] == "processing"


def test_checkpoint_idempotency(pg_connection):
    export_time = datetime(2026, 3, 15, 0, 15, tzinfo=timezone.utc)
    first = insert_checkpoint(
        pg_connection,
        source="gdelt_events_v2",
        export_time_utc=export_time,
        export_url="https://data.gdeltproject.org/gdeltv2/20260315001500.export.CSV.zip",
        export_filename="20260315001500.export.CSV.zip",
    )
    second = insert_checkpoint(
        pg_connection,
        source="gdelt_events_v2",
        export_time_utc=export_time,
        export_url="https://data.gdeltproject.org/gdeltv2/20260315001500.export.CSV.zip",
        export_filename="20260315001500.export.CSV.zip",
    )

    assert first["id"] == second["id"]


def test_batch_insert_raw_and_normalized_with_dedupe(pg_connection):
    run_id = uuid4()
    insert_ingestion_run(pg_connection, run_id=run_id, trigger_mode="manual", status="started")
    events = [
        _build_event(run_id, "dedupe-one"),
        _build_event(run_id, "dedupe-two"),
        _build_event(run_id, "dedupe-two"),
        _build_event(run_id, "dedupe-three"),
    ]

    with pg_connection.transaction():
        inserted_raw, inserted_normalized = insert_raw_and_normalized_batch(
            pg_connection,
            events=events,
            batch_size=2,
        )

    with pg_connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS c FROM raw_events")
        raw_count = cursor.fetchone()["c"]
        cursor.execute("SELECT COUNT(*) AS c FROM normalized_events")
        normalized_count = cursor.fetchone()["c"]

    assert inserted_raw == 3
    assert inserted_normalized == 3
    assert raw_count == 3
    assert normalized_count == 3


def test_checkpoint_completion_updates_run(pg_connection):
    run_id = uuid4()
    export_time = datetime(2026, 3, 15, 0, 15, tzinfo=timezone.utc)
    insert_ingestion_run(pg_connection, run_id=run_id, trigger_mode="manual", status="started")
    insert_checkpoint(
        pg_connection,
        source="gdelt_events_v2",
        export_time_utc=export_time,
        export_url="https://data.gdeltproject.org/gdeltv2/20260315001500.export.CSV.zip",
        export_filename="20260315001500.export.CSV.zip",
    )

    with pg_connection.transaction():
        mark_checkpoint_completed(
            pg_connection,
            source="gdelt_events_v2",
            export_time_utc=export_time,
            row_count_raw=10,
            row_count_inserted=9,
            row_count_duplicates=1,
        )
        update_ingestion_run(
            pg_connection,
            run_id=run_id,
            status="completed",
            exports_seen=1,
            exports_completed=1,
            events_inserted=9,
            events_duplicated=1,
            finished=True,
        )

    with pg_connection.cursor() as cursor:
        cursor.execute("SELECT status, events_inserted, events_duplicated FROM ingestion_runs WHERE id = %s", (run_id,))
        run_row = cursor.fetchone()

    assert run_row["status"] == "completed"
    assert run_row["events_inserted"] == 9
    assert run_row["events_duplicated"] == 1


def test_insert_data_quality_audit_creates_row(pg_connection):
    with pg_connection.transaction():
        insert_data_quality_audit(
            pg_connection,
            total_events=25,
            missing_actor_pct=4.0,
            missing_geo_pct=8.0,
            unknown_country_pct=12.0,
        )

    with pg_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT total_events, missing_actor_pct, missing_geo_pct, unknown_country_pct
            FROM data_quality_audit
            ORDER BY id DESC
            LIMIT 1
            """
        )
        audit_row = cursor.fetchone()

    assert audit_row["total_events"] == 25
    assert float(audit_row["missing_actor_pct"]) == 4.0
    assert float(audit_row["missing_geo_pct"]) == 8.0
    assert float(audit_row["unknown_country_pct"]) == 12.0


def test_insert_dropped_events_creates_audit_rows(pg_connection):
    run_id = uuid4()
    insert_ingestion_run(pg_connection, run_id=run_id, trigger_mode="manual", status="started")

    with pg_connection.transaction():
        inserted = insert_dropped_events(
            pg_connection,
            rows=[
                {
                    "ingestion_run_id": run_id,
                    "source": "gdelt_events_v2",
                    "export_time_utc": datetime(2026, 3, 15, 0, 15, tzinfo=timezone.utc),
                    "export_url": "https://data.gdeltproject.org/gdeltv2/20260315001500.export.CSV.zip",
                    "dedupe_key": "dedupe-drop",
                    "drop_reason": "validation_drop",
                    "error_detail": "time_future_outlier",
                    "quality_flags": ["time_future_outlier"],
                    "raw_payload": {"EventCode": "190"},
                }
            ],
        )

    with pg_connection.cursor() as cursor:
        cursor.execute("SELECT drop_reason, dedupe_key FROM dropped_events ORDER BY id DESC LIMIT 1")
        dropped_row = cursor.fetchone()

    assert inserted == 1
    assert dropped_row["drop_reason"] == "validation_drop"
    assert dropped_row["dedupe_key"] == "dedupe-drop"


def test_advisory_lock_prevents_parallel_ingest(pg_connection):
    with psycopg.connect(os.environ["TEST_DATABASE_URL"], row_factory=psycopg.rows.dict_row) as second_connection:
        assert try_acquire_ingestion_lock(pg_connection) is True
        assert try_acquire_ingestion_lock(second_connection) is False
        assert release_ingestion_lock(pg_connection) is True
        assert try_acquire_ingestion_lock(second_connection) is True
        assert release_ingestion_lock(second_connection) is True


def test_hourly_counts_match_raw_at_unaligned_edges(dashboard_client, pg_connection, dashboard_window):
    params = {**dashboard_window, "start": "2026-03-15T00:00:00.000001Z",
              "end": "2026-03-15T01:30:00Z", "include_map": "false"}
    payload = dashboard_client.get("/analytics", params=params).json()
    events = dashboard_client.get("/events", params=params).json()["rows"]
    assert payload["summary"]["total_events"] == len(events)
    assert payload["map"] is None
    params["start"] = "2026-03-15T00:10:00Z"
    params["end"] = "2026-03-15T00:40:00Z"
    assert dashboard_client.get("/analytics", params=params).json()["summary"]["total_events"] == len(
        dashboard_client.get("/events", params=params).json()["rows"],
    )


def test_rollup_tracks_duplicates_updates_and_transaction_rollback(pg_connection):
    run_id = uuid4()
    insert_ingestion_run(pg_connection, run_id=run_id, trigger_mode="manual", status="started")
    event = _build_event(run_id, "rollup")
    assert insert_raw_and_normalized_batch(pg_connection, [event, event]) == (1, 1)
    assert insert_raw_and_normalized_batch(pg_connection, [event]) == (0, 0)
    with pg_connection.transaction(force_rollback=True):
        insert_raw_and_normalized_batch(pg_connection, [dict(event, dedupe_key="aborted")])
    pg_connection.execute("UPDATE normalized_events SET country_code='CA', country_name='Canada'")
    counts = pg_connection.execute(
        "SELECT country_code, live_count, historical_count FROM event_hourly_counts ORDER BY country_code",
    ).fetchall()
    assert counts == [
        {"country_code": "CA", "live_count": 1, "historical_count": 1},
        {"country_code": "USA", "live_count": 0, "historical_count": 0},
    ]


def test_history_counts_complete_zero_and_keeps_completed_coverage(dashboard_client, pg_connection):
    end = datetime(2026, 3, 15, tzinfo=timezone.utc)
    pg_connection.execute("""
        INSERT INTO gdelt_export_checkpoints (source, export_time_utc, export_url, export_filename, status)
        SELECT 'gdelt_events_v2', stamp, 'example', 'example', 'completed'
        FROM generate_series(%s::timestamptz - interval '48 hours',
            %s::timestamptz - interval '15 minutes', interval '15 minutes') stamp
    """, (end, end))
    pg_connection.execute("UPDATE gdelt_export_checkpoints SET status='expired'")
    body = dashboard_client.get("/history", params={"end": end.isoformat(), "country": "US"}).json()
    assert body["comparison_24h"]["absolute_change"] == 0
    assert body["comparison_24h"]["percent_change"] is None
    assert body["comparison_7d"]["absolute_change"] is None
    assert body["anomaly"]["status"] == "insufficient_history"
    assert body["daily"][-1]["count"] == 0
    assert body["daily"][-3]["count"] is None


def test_map_and_profile_endpoints_are_bounded(dashboard_client, dashboard_window):
    globe = dashboard_client.get("/map", params=dashboard_window).json()
    assert globe["cell_degrees"] == 2
    assert globe["viewport_events"] == 4
    assert sum(cell["count"] for cell in globe["cells"]) == 4
    assert globe["omitted_events"] == 0
    split = dashboard_client.get("/map", params={**dashboard_window, "west": 170, "east": -170}).json()
    assert split["viewport_events"] == 1
    assert split["cell_degrees"] < 2
    assert dashboard_client.get("/map", params={"south": 60, "north": 40}).status_code == 422
    profile = dashboard_client.get("/country-profile", params={**dashboard_window, "country": "CA"})
    assert profile.status_code == 200, profile.text
    assert profile.json()["country_name"] == "Canada"
    assert profile.json()["sources"] == [{"label": "report.example", "count": 1}]
    locations = dashboard_client.get("/country-profile", params={**dashboard_window, "country": "CA", "include_sources": False})
    assert locations.json() == {**profile.json(), "sources": []}
    events = dashboard_client.get("/events", params={**dashboard_window, "country": "CA"}).json()["rows"]
    assert events[0]["geographic_precision"] == "locality"
    assert events[0]["actor1_country_name"] == "United Kingdom"
    assert dashboard_client.get("/country-profile", params=dashboard_window).status_code == 422
    catalog = dashboard_client.get("/countries").json()["rows"]
    assert {"country_code": "GB", "country_name": "Gabon"} in catalog


def test_world_map_does_not_truncate_occupied_two_degree_cells(dashboard_client, pg_connection):
    run_id = uuid4()
    insert_ingestion_run(pg_connection, run_id=run_id, trigger_mode="manual", status="started")
    common = _build_event(run_id, "unused")
    events = [
        dict(common, dedupe_key=f"map-{index}", action_geo_lat=-85 + 2 * (index // 180),
             action_geo_long=-179 + 2 * (index % 180))
        for index in range(701)
    ]
    insert_raw_and_normalized_batch(pg_connection, events)
    window = {"start": common["event_time_utc"].isoformat(),
              "end": (common["event_time_utc"] + timedelta(hours=1)).isoformat()}
    result = dashboard_client.get("/map", params=window).json()
    assert result["cell_degrees"] == 2
    assert result["viewport_events"] == len(result["cells"]) == len(events)


def test_health_monitor_counts_missing_and_reports_scheduler(dashboard_client, pg_connection, monkeypatch):
    from src.api import main as api
    monkeypatch.setattr(api, "get_connection", lambda: nullcontext(pg_connection))
    response = dashboard_client.get("/health/monitor")
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "degraded"
    assert "scheduler_stale" in response.json()["issues"]
    assert response.json()["coverage"]["missing"] == 96
    assert len(response.json()["coverage"]["latest_gaps"]) == 8
    assert response.headers["server-timing"].startswith("app;dur=")


def test_news_coverage_groups_exact_urls_and_checks_scope(dashboard_client, pg_connection, dashboard_window, monkeypatch):
    start = datetime.fromisoformat(dashboard_window["start"])
    end = datetime.fromisoformat(dashboard_window["end"])
    run_id = uuid4()
    insert_ingestion_run(pg_connection, run_id=run_id, trigger_mode="manual", status="started")
    source = "https://Example.com/story"
    rows = [
        (source, "044", "diplomacy", end - timedelta(minutes=10)),
        (source, "190", "conflict", end - timedelta(minutes=20)),
        (source + "?edition=2", "190", "conflict", end - timedelta(minutes=30)),
        ("https://elsewhere.example/other", "190", "conflict", end - timedelta(minutes=40)),
        ("https://example.com@elsewhere.example/unsafe", "190", "conflict", start),
        ("https://example.com\\elsewhere.example/unsafe", "190", "conflict", start),
        (source, "190", "conflict", end),
    ]
    common = _build_event(run_id, "unused")
    insert_raw_and_normalized_batch(pg_connection, [
        dict(common, dedupe_key=f"coverage-{index}", event_time_utc=stamp, event_code=code,
             primary_category=category, raw_payload={"EventCode": code, "SOURCEURL": url})
        for index, (url, code, category, stamp) in enumerate(rows)
    ])
    filters = {**dashboard_window, "country": "USA"}
    response = dashboard_client.get("/coverage", params=filters)
    assert response.status_code == 200, response.text
    body = response.json()
    assert (body["inspected_records"], body["linked_records"], body["unique_urls"], body["truncated"]) == (10, 4, 3, False)
    assert body["domains"][0] == {"domain": "example.com", "articles": 2, "records": 3}
    first = body["articles"][0]
    assert (first["url"], first["record_count"], first["category"], first["event_label"]) == (source, 2, "diplomacy", "Meet")

    filtered = dashboard_client.get("/coverage", params={**filters, "domain": "EXAMPLE.COM", "category": "conflict"}).json()
    assert filtered["unique_urls"] == 3
    assert [article["url"] for article in filtered["articles"]] == [source, source + "?edition=2"]
    assert dashboard_client.get("/coverage", params={**filters, "domain": "example.com/path"}).status_code == 422
    assert dashboard_client.get("/coverage", params={**filters, "country": "GB"}).json()["articles"] == []

    monkeypatch.setattr("src.ingestion.coverage_queries.RECORD_LIMIT", 2)
    bounded = dashboard_client.get("/coverage", params=filters).json()
    assert (bounded["inspected_records"], bounded["unique_urls"], bounded["truncated"]) == (2, 1, True)


def test_history_category_changes_match_raw_counts_for_complete_adjacent_windows(dashboard_client, pg_connection):
    end = datetime(2026, 3, 15, tzinfo=timezone.utc)
    split = end - timedelta(days=1)
    previous = split - timedelta(days=1)
    pg_connection.execute("""
        INSERT INTO event_history_coverage (export_time_utc)
        SELECT stamp FROM generate_series(%s::timestamptz, %s::timestamptz, interval '15 minutes') stamp
    """, (previous, end - timedelta(minutes=15)))
    run_id = uuid4()
    insert_ingestion_run(pg_connection, run_id=run_id, trigger_mode="manual", status="started")
    common = _build_event(run_id, "unused")
    insert_raw_and_normalized_batch(pg_connection, [
        dict(common, dedupe_key=f"shifts-{index}", primary_category=category, event_time_utc=stamp)
        for index, (category, stamp) in enumerate([
            ("conflict", previous), ("conflict", split), ("diplomacy", split), ("politics", end),
        ])
    ])
    body = dashboard_client.get("/history", params={"end": end.isoformat(), "country": "USA"}).json()
    shifts = body["comparison_24h"]["category_changes"]
    counts = pg_connection.execute("""
        SELECT primary_category AS category, count(*) FILTER (WHERE event_time_utc >= %s) AS current,
               count(*) FILTER (WHERE event_time_utc < %s) AS previous
        FROM normalized_events WHERE event_time_utc >= %s AND event_time_utc < %s AND country_code = 'USA'
        GROUP BY primary_category
    """, (split, split, previous, end)).fetchall()
    assert shifts == [dict(row, change=row["current"] - row["previous"])
                      for row in sorted(counts, key=lambda row: -abs(row["current"] - row["previous"]))]
    assert body["comparison_7d"]["category_changes"] == []
    pg_connection.execute("DELETE FROM event_history_coverage WHERE export_time_utc = %s", (split,))
    incomplete = dashboard_client.get("/history", params={"end": end.isoformat()}).json()
    assert incomplete["comparison_24h"]["category_changes"] == []


def test_legacy_momentum_uses_hourly_rates_on_both_sides(pg_connection):
    from src.main import MOMENTUM_SQL

    now = pg_connection.execute("SELECT now() AS stamp").fetchone()["stamp"]
    run_id = uuid4()
    insert_ingestion_run(pg_connection, run_id=run_id, trigger_mode="manual", status="started")
    common = _build_event(run_id, "unused")
    insert_raw_and_normalized_batch(pg_connection, [
        dict(common, dedupe_key=f"momentum-{offset}", event_time_utc=now - timedelta(hours=offset, minutes=30))
        for offset in range(24)
    ])
    result, = pg_connection.execute(MOMENTUM_SQL).fetchall()
    assert (result["c_3h"], result["c_24h_hourly_avg"], result["momentum_ratio"]) == (3, 1, 1)
