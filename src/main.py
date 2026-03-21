from __future__ import annotations

import argparse
import logging
import sys
from datetime import timezone
from pathlib import Path

from src.domain.events.categorization import categorize_event
from src.connectors.gdelt import fetch_latest_events
from src.db import get_connection
from src.ingestion.repository import (
    fetch_event_stats,
    fetch_recent_ingestion_runs,
    fetch_recent_normalized_events,
)
from src.pipeline.ingest_service import ingest_latest_export

EVENT_CODE_LABELS = {
    "020": "Appeal",
    "021": "Appeal for material cooperation",
    "023": "Appeal for humanitarian aid",
    "025": "Appeal for policy change",
    "030": "Express intent to cooperate",
    "031": "Express intent to engage in material cooperation",
    "032": "Express intent to provide aid",
    "040": "Consult",
    "042": "Make a visit",
    "043": "Host a visit",
    "044": "Meet",
    "050": "Engage in diplomatic cooperation",
    "051": "Praise or endorse",
    "052": "Defend verbally",
    "053": "Rally support",
    "060": "Engage in material cooperation",
    "061": "Cooperate economically",
    "062": "Provide financial assistance",
    "064": "Share intelligence or information",
    "070": "Provide aid",
    "071": "Provide humanitarian aid",
    "072": "Provide military aid",
    "080": "Yield",
    "081": "Ease sanctions",
    "084": "Release or return person/property",
    "090": "Investigate",
    "091": "Investigate crime",
    "100": "Demand",
    "101": "Demand policy change",
    "110": "Disapprove",
    "112": "Criticize",
    "120": "Reject",
    "121": "Reject proposal",
    "130": "Threaten",
    "131": "Threaten sanctions",
    "140": "Protest",
    "141": "Demonstrate or rally",
    "145": "Violent protest",
    "150": "Exhibit force",
    "151": "Increase police presence",
    "154": "Mobilize forces",
    "155": "Mobilize cyber forces",
    "170": "Coerce",
    "172": "Military attack",
    "173": "Arrest or detain",
    "174": "Expel or deport",
    "176": "Cyber attack",
    "180": "Assault",
    "181": "Conduct armed assault",
    "190": "Fight",
    "193": "Fight with small arms",
    "200": "Use unconventional violence",
}

ROOT_EVENT_LABELS = {
    "01": "Public statement",
    "02": "Appeal",
    "03": "Intent to cooperate",
    "04": "Consult",
    "05": "Diplomatic cooperation",
    "06": "Material cooperation",
    "07": "Provide aid",
    "08": "Yield",
    "09": "Investigate",
    "10": "Demand",
    "11": "Disapprove",
    "12": "Reject",
    "13": "Threaten",
    "14": "Protest",
    "15": "Exhibit force",
    "16": "Reduce relations",
    "17": "Coerce",
    "18": "Assault",
    "19": "Fight",
    "20": "Unconventional violence",
}

MAX_EVENTS_PER_CATEGORY = 3
MAX_BREAKING_EVENTS = 5

SPIKES_SQL = """
WITH events AS (
    SELECT
        event_time_utc AS event_time,
        primary_category AS category,
        country_code AS country,
        goldstein_score AS tone,
        actor1_name AS actor1,
        actor2_name AS actor2
    FROM normalized_events
),
recent AS (
    SELECT
        category,
        country,
        COUNT(*) AS recent_count
    FROM events
    WHERE event_time >= NOW() - make_interval(hours => %s)
    GROUP BY 1, 2
),
baseline_daily AS (
    SELECT
        category,
        country,
        date_trunc('day', event_time) AS day_bucket,
        COUNT(*) AS daily_count
    FROM events
    WHERE event_time >= NOW() - INTERVAL '15 days'
      AND event_time < NOW() - make_interval(hours => %s)
    GROUP BY 1, 2, 3
),
baseline_stats AS (
    SELECT
        category,
        country,
        AVG(daily_count) AS baseline_avg,
        STDDEV_POP(daily_count) AS baseline_std
    FROM baseline_daily
    GROUP BY 1, 2
)
SELECT
    r.category,
    r.country,
    r.recent_count,
    b.baseline_avg,
    ROUND(
        (r.recent_count - b.baseline_avg) / NULLIF(b.baseline_std, 0),
        2
    ) AS z_score,
    ROUND(r.recent_count / NULLIF(b.baseline_avg, 0), 2) AS lift_ratio
FROM recent r
JOIN baseline_stats b
    ON r.category = b.category
   AND r.country = b.country
WHERE r.recent_count >= 10
ORDER BY z_score DESC NULLS LAST, lift_ratio DESC
LIMIT 50
"""

TENSION_SQL = """
WITH events AS (
    SELECT
        event_time_utc AS event_time,
        primary_category AS category,
        country_code AS country,
        goldstein_score AS tone,
        actor1_name AS actor1,
        actor2_name AS actor2
    FROM normalized_events
)
SELECT
    COALESCE(actor1, 'Unknown') AS actor1,
    COALESCE(actor2, 'Unknown') AS actor2,
    category,
    COUNT(*) AS event_count,
    ROUND(AVG(tone)::numeric, 2) AS avg_tone,
    MIN(tone) AS worst_tone
FROM events
WHERE event_time >= NOW() - make_interval(hours => %s)
  AND tone IS NOT NULL
  AND tone <= -5
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 3
ORDER BY avg_tone ASC, event_count DESC
LIMIT 50
"""

MOMENTUM_SQL = """
WITH events AS (
    SELECT
        event_time_utc AS event_time,
        primary_category AS category
    FROM normalized_events
),
short_window AS (
    SELECT
        category,
        COUNT(*) AS c_3h
    FROM events
    WHERE event_time >= NOW() - INTERVAL '3 hours'
    GROUP BY 1
),
long_window AS (
    SELECT
        category,
        COUNT(*) / 8.0 AS c_24h_hourly_avg
    FROM events
    WHERE event_time >= NOW() - INTERVAL '24 hours'
    GROUP BY 1
)
SELECT
    s.category,
    s.c_3h,
    l.c_24h_hourly_avg,
    ROUND((s.c_3h / 3.0) / NULLIF(l.c_24h_hourly_avg, 0), 2) AS momentum_ratio
FROM short_window s
JOIN long_window l USING (category)
ORDER BY momentum_ratio DESC NULLS LAST, s.c_3h DESC
"""


def get_event_category(event_code: str) -> str:
    # keeping this helper for backwards compatibility with console monitor code
    return categorize_event({"event_code": event_code}).primary_category


def get_event_label(event_code: str) -> str:
    event_label = EVENT_CODE_LABELS.get(event_code)

    if not event_label:
        prefix = event_code[:2]
        event_label = ROOT_EVENT_LABELS.get(prefix, "Unknown event")

    return event_label


def get_avg_tone(event: dict[str, str]) -> float:
    try:
        return float(event.get("AvgTone", 0) or 0)
    except (TypeError, ValueError):
        return 0.0


def print_event_line(event: dict[str, str]) -> None:
    actor1 = event.get("Actor1Name") or "Unknown"
    actor2 = event.get("Actor2Name") or "Unknown"
    event_code = event.get("EventCode") or "Unknown"
    event_label = get_event_label(event_code)
    location = event.get("ActionGeo_FullName") or "Unknown"
    country = event.get("ActionGeo_CountryCode")
    lat = event.get("ActionGeo_Lat")
    lon = event.get("ActionGeo_Long")

    if not country:
        country = "Unknown"

    try:
        lat = f"{float(lat):.2f}"
        lon = f"{float(lon):.2f}"
    except (TypeError, ValueError):
        lat = "Unknown"
        lon = "Unknown"

    print(
        f"{actor1} \u2192 {actor2} | {event_label} ({event_code}) | "
        f"{location} | {country} | {lat}, {lon}"
    )


def configure_logging() -> None:
    # keeping the logs simple and readable for local runs
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def run_ingest_command() -> int:
    # this is just a thin cli wrapper now
    ingest_latest_export()
    return 0


def run_migrations() -> int:
    # this keeps migration workflow simple for local development
    try:
        from alembic import command
        from alembic.config import Config
    except ImportError as exc:  # pragma: no cover - dependency issue
        raise RuntimeError("Alembic is required for migrations. Install requirements.txt first.") from exc

    config_path = Path(__file__).resolve().parents[1] / "alembic.ini"
    config = Config(str(config_path))
    command.upgrade(config, "head")
    return 0


def _translation_tier(event_code: str | None) -> str:
    if not event_code:
        return "unknown"

    cleaned = event_code.strip()
    if not cleaned:
        return "unknown"

    if cleaned in EVENT_CODE_LABELS:
        return "exact"

    if cleaned[:2] in ROOT_EVENT_LABELS:
        return "root"

    return "unknown"


def _format_utc(timestamp: object) -> str:
    if timestamp is None:
        return "Unknown"
    if hasattr(timestamp, "astimezone"):
        return timestamp.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return str(timestamp)


def _format_percent(count: int, total: int) -> str:
    if total <= 0:
        return "0.00%"
    return f"{(count / total) * 100:.2f}%"


def run_latest_command(limit: int) -> int:
    with get_connection() as connection:
        rows = fetch_recent_normalized_events(connection, limit=limit)

    print(f"Latest normalized events (limit={max(int(limit), 1)})")
    if not rows:
        print("No rows found in normalized_events.")
        return 0

    for row in rows:
        event_code = row.get("event_code")
        event_label = get_event_label(event_code or "")
        secondary = row.get("secondary_category")
        if secondary:
            category = f"{row.get('primary_category')}:{secondary}"
        else:
            category = row.get("primary_category")
        print(
            f"{_format_utc(row.get('event_time_utc'))} | "
            f"{row.get('actor1_name') or 'Unknown'} -> {row.get('actor2_name') or 'Unknown'} | "
            f"{event_label} ({event_code or 'Unknown'}) | "
            f"{row.get('country_code') or 'Unknown'} | "
            f"cat={category or 'Unknown'} conf={row.get('category_confidence')} "
            f"tone={row.get('goldstein_score')}"
        )

    return 0


def run_runs_command(limit: int) -> int:
    with get_connection() as connection:
        rows = fetch_recent_ingestion_runs(connection, limit=limit)

    print(f"Recent ingestion runs (limit={max(int(limit), 1)})")
    if not rows:
        print("No ingestion runs found.")
        return 0

    for row in rows:
        error_summary = row.get("error_summary")
        error_suffix = f" | error={error_summary}" if error_summary else ""
        print(
            f"{row.get('id')} | status={row.get('status')} trigger={row.get('trigger_mode')} | "
            f"started={_format_utc(row.get('started_at'))} finished={_format_utc(row.get('finished_at'))} | "
            f"exports={row.get('exports_completed')}/{row.get('exports_seen')} "
            f"inserted={row.get('events_inserted')} dup={row.get('events_duplicated')}"
            f"{error_suffix}"
        )

    return 0


def run_stats_command(hours: int) -> int:
    with get_connection() as connection:
        stats = fetch_event_stats(connection, hours=hours)

    overview = stats["overview"]
    total_events = int(overview.get("total_events") or 0)
    missing_actor_count = int(overview.get("missing_actor_count") or 0)
    missing_geo_count = int(overview.get("missing_geo_count") or 0)
    fallback_unknown_category_count = int(overview.get("fallback_unknown_category_count") or 0)

    exact_count = 0
    root_count = 0
    unknown_code_count = 0
    for event_code_row in stats["event_code_counts"]:
        event_code = event_code_row.get("event_code")
        count = int(event_code_row.get("count") or 0)
        tier = _translation_tier(event_code)
        if tier == "exact":
            exact_count += count
        elif tier == "root":
            root_count += count
        else:
            unknown_code_count += count

    print(f"Event stats for last {stats['hours']} hour(s)")
    print(f"total_events={total_events}")
    print(
        f"missing_actor={missing_actor_count} ({_format_percent(missing_actor_count, total_events)}) "
        f"missing_geo={missing_geo_count} ({_format_percent(missing_geo_count, total_events)})"
    )
    print(
        "translation_coverage "
        f"exact={exact_count} ({_format_percent(exact_count, total_events)}) "
        f"root={root_count} ({_format_percent(root_count, total_events)}) "
        f"unknown={unknown_code_count} ({_format_percent(unknown_code_count, total_events)}) "
        f"category_fallback_unknown={fallback_unknown_category_count} "
        f"({_format_percent(fallback_unknown_category_count, total_events)})"
    )

    print("top_categories:")
    for row in stats["category_counts"]:
        print(f"- {row.get('primary_category')}: {row.get('count')}")

    print("top_countries:")
    for row in stats["top_countries"]:
        print(f"- {row.get('country_code') or 'Unknown'}: {row.get('count')}")

    return 0


def run_spikes_command(hours: int) -> int:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(SPIKES_SQL, (max(int(hours), 1), max(int(hours), 1)))
            rows = list(cursor.fetchall())

    print(f"Spikes (recent window={max(int(hours), 1)} hour(s))")
    if not rows:
        print("No spike rows found.")
        return 0

    for row in rows:
        print(
            f"{row.get('category') or 'unknown'} | "
            f"{row.get('country') or 'Unknown'} | "
            f"recent={row.get('recent_count')} baseline={row.get('baseline_avg')} "
            f"z={row.get('z_score')} lift={row.get('lift_ratio')}"
        )
    return 0


def run_tension_command(hours: int) -> int:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(TENSION_SQL, (max(int(hours), 1),))
            rows = list(cursor.fetchall())

    print(f"High-tension events (window={max(int(hours), 1)} hour(s))")
    if not rows:
        print("No high-tension rows found.")
        return 0

    for row in rows:
        print(
            f"{row.get('actor1')} -> {row.get('actor2')} | "
            f"{row.get('category') or 'unknown'} | "
            f"count={row.get('event_count')} avg_tone={row.get('avg_tone')} "
            f"worst_tone={row.get('worst_tone')}"
        )
    return 0


def run_momentum_command() -> int:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(MOMENTUM_SQL)
            rows = list(cursor.fetchall())

    print("Category momentum")
    if not rows:
        print("No momentum rows found.")
        return 0

    for row in rows:
        print(
            f"{row.get('category') or 'unknown'} | "
            f"last_3h={row.get('c_3h')} "
            f"hourly_avg_24h={row.get('c_24h_hourly_avg')} "
            f"momentum={row.get('momentum_ratio')}"
        )
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Global News Monitor CLI")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("monitor", help="Fetch latest export and print categorized events")
    subparsers.add_parser("ingest", help="Ingest latest export into PostgreSQL")
    subparsers.add_parser("migrate", help="Apply Alembic migrations")

    latest_parser = subparsers.add_parser("latest", help="Show latest normalized events from DB")
    latest_parser.add_argument("--limit", type=int, default=20)

    runs_parser = subparsers.add_parser("runs", help="Show recent ingestion runs")
    runs_parser.add_argument("--limit", type=int, default=10)

    stats_parser = subparsers.add_parser("stats", help="Show quality and translation stats")
    stats_parser.add_argument("--hours", type=int, default=24)

    spikes_parser = subparsers.add_parser("spikes", help="Show category-country spike candidates")
    spikes_parser.add_argument("--hours", type=int, default=24)

    tension_parser = subparsers.add_parser("tension", help="Show high-tension actor interactions")
    tension_parser.add_argument("--hours", type=int, default=48)

    subparsers.add_parser("momentum", help="Show category momentum ratios")

    return parser


def run_console_monitor() -> int:
    print("Global News Monitor starting...")

    events = fetch_latest_events()
    grouped_events = {
        "diplomacy": [],
        "economics": [],
        "politics": [],
        "protest": [],
        "conflict": [],
        "cyber": [],
        "crisis": [],
    }

    high_tension_events = []

    for event in events:
        event_code = event.get("EventCode") or ""
        category = categorize_event(event).primary_category
        avg_tone = get_avg_tone(event)

        if (
            avg_tone < -5
            or event_code.startswith(("17", "18", "19", "20"))
            or category == "protest"
        ):
            high_tension_events.append(event)

        if category not in grouped_events:
            continue

        if len(grouped_events[category]) >= MAX_EVENTS_PER_CATEGORY:
            continue

        grouped_events[category].append(event)

    high_tension_events.sort(key=get_avg_tone)

    if high_tension_events:
        print("\n[BREAKING / HIGH TENSION]")
        for event in high_tension_events[:MAX_BREAKING_EVENTS]:
            print_event_line(event)

    for category, category_events in grouped_events.items():
        if not category_events:
            continue

        print(f"\n[{category.upper()}]")
        for event in category_events:
            print_event_line(event)

    return 0


def main() -> int:
    configure_logging()
    args = build_arg_parser().parse_args(sys.argv[1:])
    command = args.command or "monitor"

    if command == "latest":
        return run_latest_command(limit=args.limit)
    if command == "runs":
        return run_runs_command(limit=args.limit)
    if command == "stats":
        return run_stats_command(hours=args.hours)
    if command == "spikes":
        return run_spikes_command(hours=args.hours)
    if command == "tension":
        return run_tension_command(hours=args.hours)
    if command == "momentum":
        return run_momentum_command()
    if command == "ingest":
        return run_ingest_command()
    if command == "migrate":
        return run_migrations()

    return run_console_monitor()


if __name__ == "__main__":
    raise SystemExit(main())
