"""Exercise v1.0 -> v1.1 -> v1.0 on a disposable PostgreSQL database."""

import os
from uuid import uuid4

from alembic import command
from alembic.config import Config
import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
import pytest
from sqlalchemy.exc import ProgrammingError


V10 = "0007_idempotency_hardening"


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
                    command.upgrade(config, "head")
                assert connection.execute("TABLE alembic_version").fetchone()["version_num"] == V10
                assert connection.execute("""
                    SELECT country_name, event_code FROM normalized_events n
                    WHERE EXISTS (SELECT FROM raw_events r WHERE r.id=n.raw_event_id AND r.ingestion_run_id=%s)
                """, (run,)).fetchone() == {"country_name": "United Kingdom", "event_code": None}
            finally:
                connection.execute("DROP TABLE event_hourly_counts")
            command.upgrade(config, "head")
            report = connection.execute("TABLE gdelt_code_correction_report").fetchone()
            assert report["restored_event_codes"] >= 1
            assert report["country_name_changed"] >= 1
            rows = connection.execute("""
                SELECT n.event_code, n.country_name, r.raw_payload ->> 'Actor1CountryCode' AS actor
                FROM normalized_events n JOIN raw_events r ON r.id = n.raw_event_id WHERE r.ingestion_run_id = %s
            """, (run,)).fetchall()
            assert rows == [{"event_code": "0211", "country_name": "Gabon", "actor": "GBR"}]
            command.upgrade(config, "head")
            command.downgrade(config, V10)
            assert connection.execute("SELECT to_regclass('event_hourly_counts') AS tab").fetchone()["tab"] is None
            assert connection.execute("""
                SELECT event_code FROM raw_events WHERE ingestion_run_id = %s
            """, (run,)).fetchone()["event_code"] == "0211"
            command.upgrade(config, "head")
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
