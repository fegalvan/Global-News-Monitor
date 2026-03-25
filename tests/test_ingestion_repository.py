from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

from src.ingestion.repository import (
    insert_data_quality_audit,
    insert_checkpoint,
    insert_raw_and_normalized_batch,
    reset_stale_processing_checkpoints,
)


class FakeCursor:
    def __init__(self, raw_insert_results=None, selected_checkpoint=None, update_rowcount=0):
        self.raw_insert_results = list(raw_insert_results or [])
        self.selected_checkpoint = selected_checkpoint
        self.update_rowcount = update_rowcount
        self.executed = []
        self.rowcount = 0
        self._last_fetchone = None
        self._last_fetchall = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=()):
        self.executed.append((query, params))
        normalized_query = " ".join(query.split())

        if "INSERT INTO raw_events" in normalized_query:
            returned_rows = []
            while self.raw_insert_results:
                result = self.raw_insert_results.pop(0)
                if result is None:
                    continue
                result = dict(result)
                result.setdefault("source", "gdelt_events_v2")
                result.setdefault("dedupe_key", "")
                returned_rows.append(result)
            self._last_fetchall = returned_rows
            self.rowcount = len(returned_rows)
            return

        if "INSERT INTO normalized_events" in normalized_query:
            self._last_fetchone = None
            self.rowcount = len(params) // 15 if params else 0
            return

        if "INSERT INTO data_quality_audit" in normalized_query:
            self._last_fetchone = None
            self.rowcount = 1
            return

        if "SELECT * FROM gdelt_export_checkpoints" in normalized_query:
            self._last_fetchone = self.selected_checkpoint
            return

        if "UPDATE gdelt_export_checkpoints" in normalized_query:
            self._last_fetchone = None
            self.rowcount = self.update_rowcount
            return

        self._last_fetchone = None

    def fetchone(self):
        return self._last_fetchone

    def fetchall(self):
        return list(self._last_fetchall)


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _build_event(dedupe_key: str) -> dict:
    return {
        "source": "gdelt_events_v2",
        "export_time_utc": datetime(2026, 3, 15, 0, 15, tzinfo=timezone.utc),
        "export_url": "https://data.gdeltproject.org/gdeltv2/20260315001500.export.CSV.zip",
        "ingestion_run_id": uuid4(),
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


def test_insert_raw_and_normalized_batch_skips_duplicates():
    fake_cursor = FakeCursor(
        raw_insert_results=[
            {"id": 10, "source": "gdelt_events_v2", "dedupe_key": "one"},
            None,
            {"id": 11, "source": "gdelt_events_v2", "dedupe_key": "three"},
        ]
    )
    connection = FakeConnection(fake_cursor)
    events = [_build_event("one"), _build_event("two"), _build_event("three")]

    inserted_raw, inserted_normalized = insert_raw_and_normalized_batch(connection, events)

    assert inserted_raw == 2
    assert inserted_normalized == 2

    normalized_insert_count = sum(
        1 for query, _ in fake_cursor.executed if "INSERT INTO normalized_events" in query
    )
    assert normalized_insert_count == 1


def test_insert_checkpoint_query_is_idempotent():
    fake_cursor = FakeCursor(
        selected_checkpoint={
            "source": "gdelt_events_v2",
            "status": "pending",
            "export_time_utc": datetime(2026, 3, 15, 0, 15, tzinfo=timezone.utc),
        }
    )
    connection = FakeConnection(fake_cursor)

    checkpoint = insert_checkpoint(
        connection=connection,
        source="gdelt_events_v2",
        export_time_utc=datetime(2026, 3, 15, 0, 15, tzinfo=timezone.utc),
        export_url="https://data.gdeltproject.org/gdeltv2/20260315001500.export.CSV.zip",
        export_filename="20260315001500.export.CSV.zip",
    )

    assert checkpoint["status"] == "pending"
    insert_query = fake_cursor.executed[0][0]
    assert "ON CONFLICT (source, export_time_utc) DO NOTHING" in insert_query


def test_reset_stale_processing_checkpoints_returns_updated_rows():
    fake_cursor = FakeCursor(update_rowcount=3)
    connection = FakeConnection(fake_cursor)

    updated_rows = reset_stale_processing_checkpoints(
        connection=connection,
        source="gdelt_events_v2",
        stale_after=timedelta(minutes=30),
    )

    assert updated_rows == 3


def test_insert_data_quality_audit_writes_one_row():
    fake_cursor = FakeCursor()
    connection = FakeConnection(fake_cursor)

    insert_data_quality_audit(
        connection,
        total_events=100,
        missing_actor_pct=10.0,
        missing_geo_pct=20.0,
        unknown_country_pct=30.0,
    )

    audit_insert_count = sum(
        1 for query, _ in fake_cursor.executed if "INSERT INTO data_quality_audit" in query
    )
    assert audit_insert_count == 1
