from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4
import os

import psycopg
import pytest
from alembic import command
from alembic.config import Config
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
                "TRUNCATE TABLE dropped_events, data_quality_audit, normalized_events, raw_events, gdelt_export_checkpoints, ingestion_runs RESTART IDENTITY CASCADE"
            )
        yield


def test_migrations_reach_head(pg_connection):
    with pg_connection.cursor() as cursor:
        cursor.execute("SELECT version_num FROM alembic_version")
        assert cursor.fetchone()["version_num"] == "0007_idempotency_hardening"
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
             action_geo_lat=90, action_geo_long=180, event_time_utc=start + timedelta(hours=1)),
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
