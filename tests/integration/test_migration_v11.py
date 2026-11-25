"""Rehearse release migrations and rollback on a disposable PostgreSQL database."""

import os
from datetime import datetime, timezone
from uuid import uuid4

from alembic import command
from alembic.config import Config
import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
import pytest
from sqlalchemy.exc import ProgrammingError


V10 = "0007_idempotency_hardening"
V11 = "0011_map_index"


def test_quality_migration_and_rollback():
    url = os.getenv("TEST_MIGRATION_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_MIGRATION_DATABASE_URL to the dedicated loopback migration database.")
    target = conninfo_to_dict(url)
    assert target.get("host") in {"127.0.0.1", "localhost", "::1"}
    assert target.get("dbname") == "gnm_migration_test"

    config = Config("alembic.ini")
    config.attributes["database_url"] = url
    with psycopg.connect(url, autocommit=True, row_factory=dict_row) as connection:
        if connection.execute("SELECT to_regclass('alembic_version') AS version").fetchone()["version"]:
            command.downgrade(config, V10)
        command.upgrade(config, V10)
        run = uuid4()
        connection.execute("""
            INSERT INTO ingestion_runs (id, status, trigger_mode) VALUES (%s, 'completed', 'manual')
        """, (run,))
        connection.execute("""
            WITH new_raw AS (
                INSERT INTO raw_events (export_time_utc, export_url, ingestion_run_id, dedupe_key,
                    event_time_utc, raw_payload, validation_flags)
                VALUES ('2026-11-24T12:00Z','fixture',%s,%s,
                    '2026-11-24T12:00Z','{"EventCode":"0211","ActionGeo_CountryCode":"GB","Actor1CountryCode":"GBR"}',
                    '["bad_event_code"]')
                RETURNING id, event_time_utc
            )
            INSERT INTO normalized_events (raw_event_id, event_time_utc, country_code, country_name,
                primary_category, category_confidence, category_reason, source)
            SELECT id, event_time_utc, 'GB', 'United Kingdom', 'diplomacy', 0.82, 'fixture', 'gdelt_events_v2'
            FROM new_raw
        """, (run, str(run)))
        try:
            connection.execute("CREATE TABLE event_hourly_counts (blocker int)")
            try:
                with pytest.raises(ProgrammingError, match="event_hourly_counts.*already exists"):
                    command.upgrade(config, V11)
                assert connection.execute("TABLE alembic_version").fetchone()["version_num"] == V10
                assert connection.execute("""
                    SELECT country_name, event_code FROM normalized_events n
                    WHERE EXISTS (SELECT FROM raw_events r WHERE r.id=n.raw_event_id AND r.ingestion_run_id=%s)
                """, (run,)).fetchone() == {"country_name": "United Kingdom", "event_code": None}
            finally:
                connection.execute("DROP TABLE event_hourly_counts")
            command.upgrade(config, V11)
            report = connection.execute("TABLE gdelt_code_correction_report").fetchone()
            assert report["restored_event_codes"] >= 1
            assert report["country_name_changed"] >= 1
            rows = connection.execute("""
                SELECT n.event_code, n.country_name, r.raw_payload ->> 'Actor1CountryCode' AS actor
                FROM normalized_events n JOIN raw_events r ON r.id = n.raw_event_id WHERE r.ingestion_run_id = %s
            """, (run,)).fetchall()
            assert rows == [{"event_code": "0211", "country_name": "Gabon", "actor": "GBR"}]
            command.upgrade(config, V11)
            command.downgrade(config, V10)
            assert connection.execute("SELECT to_regclass('event_hourly_counts') AS tab").fetchone()["tab"] is None
            assert connection.execute("""
                SELECT event_code FROM raw_events WHERE ingestion_run_id = %s
            """, (run,)).fetchone()["event_code"] == "0211"
            command.upgrade(config, V11)
            report = connection.execute("TABLE gdelt_code_correction_report").fetchone()
            assert report["restored_event_codes"] == 0
            assert report["country_name_changed"] == 0
        finally:
            with connection.transaction():
                connection.execute("""
                    DELETE FROM normalized_events WHERE raw_event_id IN
                        (SELECT id FROM raw_events WHERE ingestion_run_id = %s)
                """, (run,))
                connection.execute("DELETE FROM raw_events WHERE ingestion_run_id = %s", (run,))
                connection.execute("DELETE FROM ingestion_runs WHERE id = %s", (run,))


def test_v12_reclassification_updates_rollups_and_reverses_before_ingestion(monkeypatch):
    url = os.getenv("TEST_MIGRATION_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_MIGRATION_DATABASE_URL to the dedicated loopback migration database.")
    target = conninfo_to_dict(url)
    assert target.get("host") in {"127.0.0.1", "localhost", "::1"}
    assert target.get("dbname") == "gnm_migration_test"
    config = Config("alembic.ini")
    config.attributes["database_url"] = url
    command.upgrade(config, V11)
    run = uuid4()
    payload = {"EventCode": "128", "SOURCEURL": "https://example.org/factory-breaches-wage-rules"}
    from psycopg.types.json import Jsonb

    archived = datetime(2026, 11, 21, 17, tzinfo=timezone.utc)
    with psycopg.connect(url, autocommit=True, row_factory=dict_row) as connection:
        connection.execute("INSERT INTO ingestion_runs (id, status, trigger_mode) VALUES (%s, 'completed', 'manual')", (run,))
        event_id = connection.execute("""
            WITH source_row AS (
                INSERT INTO raw_events (export_time_utc, export_url, ingestion_run_id, dedupe_key, event_time_utc, raw_payload)
                VALUES ('2026-11-22T10:00Z', 'fixture', %s, %s, '2026-11-22T10:00Z', %s)
                RETURNING id, event_time_utc
            )
            INSERT INTO normalized_events (raw_event_id, event_time_utc, event_code, country_code, country_name,
                     primary_category, category_confidence, category_reason, source)
            SELECT id, event_time_utc, '128', 'MY', 'Malaysia', 'cyber', 0.9, 'cyber_keyword:breach', 'gdelt_events_v2'
            FROM source_row RETURNING event_id
        """, (run, str(run), Jsonb(payload))).fetchone()["event_id"]
        connection.execute("INSERT INTO event_hourly_counts VALUES (%s, 'MY', 'Malaysia', 'cyber', 0, 0, 2)", (archived,))
        try:
            from src.domain.events import categorization

            def fail_reclassification(_row):
                raise RuntimeError("Interrupted classification")

            with monkeypatch.context() as patch:
                patch.setattr(categorization, "categorize_event_v12", fail_reclassification)
                with pytest.raises(RuntimeError, match="Interrupted classification"):
                    command.upgrade(config, "head")

            assert connection.execute("SELECT version_num FROM alembic_version").fetchone()["version_num"] == V11
            assert connection.execute("SELECT to_regclass('gdelt_event_context') AS tab").fetchone()["tab"] is None
            assert connection.execute("SELECT primary_category FROM normalized_events WHERE event_id = %s",
                                      (event_id,)).fetchone()["primary_category"] == "cyber"

            command.upgrade(config, "head")
            revision = connection.execute("TABLE event_category_revision").fetchone()
            assert revision["corrected_from"] == datetime(2026, 11, 21, 18, tzinfo=timezone.utc)
            assert revision["changed_records"] == 1
            assert connection.execute("""
                SELECT n.primary_category, r.raw_payload FROM normalized_events n JOIN raw_events r ON r.id = n.raw_event_id
                WHERE n.event_id = %s
            """, (event_id,)).fetchone() == {"primary_category": "politics", "raw_payload": payload}
            assert connection.execute("""
                SELECT primary_category, sum(historical_count)::int AS n FROM event_hourly_counts
                WHERE country_code = 'MY' AND historical_count > 0 GROUP BY 1 ORDER BY 1
            """).fetchall() == [{"primary_category": "cyber", "n": 2}, {"primary_category": "politics", "n": 1}]

            command.downgrade(config, V11)
            assert connection.execute("SELECT primary_category FROM normalized_events WHERE event_id = %s",
                                      (event_id,)).fetchone()["primary_category"] == "cyber"
            assert connection.execute("SELECT sum(historical_count) AS n FROM event_hourly_counts WHERE country_code = 'MY' AND primary_category = 'cyber'").fetchone()["n"] == 3

            command.upgrade(config, "head")
            last_id = connection.execute("SELECT last_value FROM normalized_events_event_id_seq").fetchone()["last_value"]
            connection.execute("SELECT nextval('normalized_events_event_id_seq')")
            try:
                from sqlalchemy.exc import DBAPIError

                with pytest.raises(DBAPIError, match="Events have been written or pruned"):
                    command.downgrade(config, V11)
                assert connection.execute("SELECT version_num FROM alembic_version").fetchone()["version_num"] == "0012_category_quality"
            finally:
                connection.execute("SELECT setval('normalized_events_event_id_seq', %s)", (last_id,))
        finally:
            if connection.execute("SELECT to_regclass('event_category_revision') AS tab").fetchone()["tab"]:
                command.downgrade(config, V11)
            raw_id = connection.execute("DELETE FROM normalized_events WHERE event_id = %s RETURNING raw_event_id",
                                        (event_id,)).fetchone()["raw_event_id"]
            connection.execute("DELETE FROM raw_events WHERE id = %s", (raw_id,))
            connection.execute("DELETE FROM ingestion_runs WHERE id = %s", (run,))
            connection.execute("DELETE FROM event_hourly_counts WHERE hour_utc = %s AND country_code = 'MY'", (archived,))
