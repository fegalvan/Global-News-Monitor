from __future__ import annotations

import logging
import sys
from pathlib import Path

from src.domain.events.categorization import categorize_event
from src.connectors.gdelt import fetch_latest_events
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
    command = sys.argv[1] if len(sys.argv) > 1 else "monitor"

    if command == "ingest":
        return run_ingest_command()
    if command == "migrate":
        return run_migrations()

    return run_console_monitor()


if __name__ == "__main__":
    raise SystemExit(main())
