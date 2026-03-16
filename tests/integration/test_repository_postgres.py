from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4
import os

import psycopg
import pytest

from src.ingestion.repository import (
    claim_checkpoint,
    insert_checkpoint,
    insert_ingestion_run,
    insert_raw_and_normalized_batch,
    mark_checkpoint_completed,
    update_ingestion_run,
)


def _load_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


@pytest.fixture(scope="session")
def pg_connection():
    database_url = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not database_url:
        pytest.skip("Set TEST_DATABASE_URL or DATABASE_URL for Postgres integration tests.")

    connection = psycopg.connect(database_url, row_factory=psycopg.rows.dict_row)
    root = Path(__file__).resolve().parents[2]

    with connection.transaction():
        # we apply schemas directly so integration tests validate real repository SQL
        with connection.cursor() as cursor:
            cursor.execute(_load_sql(root / "sql" / "stage1_schema.sql"))
            cursor.execute(_load_sql(root / "sql" / "stage2_schema.sql"))

    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def clean_tables(pg_connection):
    with pg_connection.transaction():
        with pg_connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE normalized_events, raw_events, gdelt_export_checkpoints, ingestion_runs RESTART IDENTITY CASCADE")
    yield


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
