"""Benchmark v1.0 and v1.1 with the same deterministic, synthetic PostgreSQL data."""

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
from statistics import median
import subprocess
from time import perf_counter
from unittest.mock import patch

from alembic import command
from alembic.config import Config
import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

from src.db import fetch_all
from src.ingestion.dashboard_queries import EventFilters, VALID_COORDINATES, fetch_analytics, fetch_viewport

ROOT = Path(__file__).resolve().parents[1]
BASE = "79e672f8f9ffd8ee97e4dd5fed3e21ea49bf8304"
END = datetime(2026, 11, 24, 0, 0, tzinfo=timezone.utc)
RECORDS = 1_750_000


def seed(connection, start):
    connection.execute("""
        INSERT INTO ingestion_runs (id, status, trigger_mode)
        VALUES ('00000000-0000-0000-0000-000000000001', 'completed', 'manual');
    """)
    connection.execute("""
        WITH synthetic AS (
            SELECT i, %s::timestamptz + ((i::bigint * 103307) %% 1468800) * interval '1 second' AS t
            FROM generate_series(1, %s) i
        )
        INSERT INTO raw_events (id, source, export_time_utc, export_url, ingestion_run_id, dedupe_key,
                                global_event_id, sql_date, event_time_utc, raw_payload, event_code, action_geo_country_code)
        SELECT i, 'gdelt_events_v2', date_bin('15 minutes', t, %s), 'synthetic',
               '00000000-0000-0000-0000-000000000001', i::text, i, (t AT TIME ZONE 'UTC')::date, t, '{}'::jsonb, '190',
               (ARRAY['US','US','US','UK','IN','IN','CH','RS','UP','BR','GM','NI','AS','ZA','JA','EG',NULL])[(i %% 17) + 1]
        FROM synthetic
    """, (start, RECORDS, start))
    connection.execute("""
        WITH synthetic AS (
            SELECT id, event_time_utc,
                (ARRAY['US','US','US','UK','IN','IN','CH','RS','UP','BR','GM','NI','AS','ZA','JA','EG',NULL])
                    [(id % 17)::int + 1] AS country,
                (ARRAY['conflict','politics','diplomacy','economics','protest','crisis','cyber'])
                    [(id % 7)::int + 1] AS category
            FROM raw_events
        )
        INSERT INTO normalized_events (
            raw_event_id, event_time_utc, event_code, country_code, country_name, latitude, longitude,
            primary_category, category_confidence, category_reason, source
        )
        SELECT id, event_time_utc, '190', country, coalesce(country, 'Unknown'),
               CASE WHEN id % 9 <> 0 THEN (id * 61 % 171000) / 1000.0 - 85.5 END,
               CASE WHEN id % 9 <> 0 THEN (id * 37 % 360000) / 1000.0 - 180 END,
               category, .82, 'synthetic', 'gdelt_events_v2'
        FROM synthetic;
    """)
    connection.execute("""
        INSERT INTO gdelt_export_checkpoints (source, export_time_utc, export_url, export_filename, status)
        SELECT 'gdelt_events_v2', slot, 'synthetic', 'synthetic', 'completed'
        FROM generate_series(%s, %s - interval '15 minutes', interval '15 minutes') slot
    """, (start, END))


def legacy_query():
    module = subprocess.check_output(
        ["git", "show", f"{BASE}:src/ingestion/dashboard_queries.py"], text=True, cwd=ROOT,
    )
    return re.search(r'def fetch_analytics.*?f"""(.*?)"""', module, re.S)[1]


def measure(call, runs):
    call()
    trials = []
    for _ in range(runs):
        start = perf_counter()
        result = call()
        trials.append(1000 * (perf_counter() - start))
    return {"median_ms": round(median(trials), 2), "min_ms": round(min(trials), 2),
            "max_ms": round(max(trials), 2)}, result


def explain(connection, sql, params):
    return connection.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql,
                              params).fetchone()["QUERY PLAN"][0]["Plan"]


def temporary_blocks(plan):
    return {mode: plan.get(f"Temp {mode} Blocks", 0) for mode in ("Read", "Written")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database_url", help="Empty disposable localhost database named gnm_bench")
    parser.add_argument("--runs", type=int, default=7)
    parser.add_argument("--reuse", action="store_true", help="Measure a benchmark dataset already seeded by this script")
    args = parser.parse_args()
    target = conninfo_to_dict(args.database_url)
    if target.get("host") not in {"localhost", "127.0.0.1", "::1"} or target.get("dbname") != "gnm_bench":
        parser.error("Use the disposable loopback database gnm_bench.")
    if args.runs < 1:
        parser.error("Runs must be positive.")
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.attributes["database_url"] = args.database_url
    sql_template = legacy_query()
    with psycopg.connect(args.database_url, autocommit=True, row_factory=dict_row) as connection:
        if args.reuse:
            if connection.execute("SELECT count(*) AS events FROM normalized_events WHERE category_reason = 'synthetic'"
                                  ).fetchone()["events"] != RECORDS:
                parser.error("The database must contain the expected seeded dataset.")
        else:
            if connection.execute("SELECT to_regclass('raw_events') AS installed").fetchone()["installed"]:
                parser.error("The database must be empty before benchmarking.")
            command.upgrade(cfg, "0007_idempotency_hardening")
            seed(connection, END - timedelta(days=17))
            connection.execute("ANALYZE")
        command.upgrade(cfg, "head")
        connection.execute("VACUUM (ANALYZE) normalized_events")
        connection.execute("SET work_mem = '4MB'; SET max_parallel_workers_per_gather = 0; ANALYZE")
        sizes = connection.execute("""
            SELECT count(*) AS aggregate_rows,
                   pg_total_relation_size('event_hourly_counts') AS aggregate_bytes,
                   pg_relation_size('idx_events_map_coordinates') AS map_index_bytes
            FROM event_hourly_counts
        """).fetchone()
        print(f"Synthetic records={RECORDS}; {sizes}", flush=True)
        for days, bucket in ((1, "hour"), (17, "day")):
            filters = EventFilters(END - timedelta(days=days), END, None, None)
            where, params = filters.where()
            sql = sql_template.format(where=where, VALID_COORDINATES=VALID_COORDINATES)
            legacy_params = (*params, bucket, 250, 500, bucket, filters.start, filters.end,
                             "gdelt_events_v2", filters.start, filters.end)
            before, old = measure(lambda: connection.execute(sql, legacy_params).fetchone(), args.runs)
            same_contract, new = measure(lambda: fetch_analytics(connection, filters, bucket=bucket), args.runs)
            core, _ = measure(lambda: fetch_analytics(connection, filters, bucket=bucket, include_map=False), args.runs)
            viewport, map_cells = measure(lambda: fetch_viewport(connection, filters, (-90, -180, 90, 180), 15), args.runs)
            for key in ("total_events", "geolocated_events", "countries_with_events", "map_cells"):
                assert old[key] == new[key], key
            for key in ("country_counts", "category_counts"):
                assert old[key] == new[key], key
            for key in ("series", "coverage"):
                for row in old[key]:
                    row["bucket_start"] = datetime.fromisoformat(row["bucket_start"])
                assert old[key] == new[key], key
            assert sum(row["count"] for row in map_cells) == new["geolocated_events"]
            plans = []

            def fetch_with_plan(conn, query, parameters):
                plans.append(temporary_blocks(explain(conn, query, parameters)))
                return fetch_all(conn, query, parameters)
            with patch("src.ingestion.dashboard_queries.fetch_all", side_effect=fetch_with_plan):
                fetch_analytics(connection, filters, bucket=bucket, include_map=False)
                fetch_viewport(connection, filters, (-90, -180, 90, 180), 15)
            plan = explain(connection, sql, legacy_params)
            print(f"{days} day(s) legacy={before} v1.1_same_contract={same_contract} "
                  f"v1.1_without_map={core} viewport={viewport} legacy_temp={temporary_blocks(plan)} "
                  f"v1.1_temp={plans}", flush=True)


if __name__ == "__main__":
    main()
