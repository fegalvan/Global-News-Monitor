from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4
import os

import psycopg
import pytest
from alembic import command
from alembic.config import Config

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
