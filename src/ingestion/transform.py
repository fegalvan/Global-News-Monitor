"""Helpers for normalizing GDELT event rows for PostgreSQL insertion."""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

from src.domain.events.categorization import categorize_event

SOURCE = "gdelt_events_v2"
WHITESPACE_PATTERN = re.compile(r"\s+")


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    return text


def _normalize_text(value: Any) -> str:
    text = _clean_text(value)
    if text is None:
        return ""

    return WHITESPACE_PATTERN.sub(" ", text).casefold()


def _parse_sql_date(value: Any) -> date | None:
    text = _clean_text(value)
    if text is None:
        return None

    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def _parse_date_added(value: Any) -> datetime | None:
    text = _clean_text(value)
    if text is None:
        return None

    try:
        return datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_decimal(value: Any) -> Decimal | None:
    text = _clean_text(value)
    if text is None:
        return None

    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _parse_int(value: Any) -> int | None:
    text = _clean_text(value)
    if text is None:
        return None

    try:
        return int(text)
    except ValueError:
        return None


def build_dedupe_key(event: dict[str, Any]) -> str:
    """Build a deterministic dedupe key for an event row."""

    global_event_id = _parse_int(event.get("GLOBALEVENTID"))
    if global_event_id is not None:
        return f"gdelt-global-event-id:{global_event_id}"

    composite_parts = [
        _normalize_text(event.get("SQLDATE")),
        _normalize_text(event.get("Actor1Name")),
        _normalize_text(event.get("Actor2Name")),
        _normalize_text(event.get("EventCode")),
        _normalize_text(event.get("ActionGeo_FullName")),
        _normalize_text(event.get("ActionGeo_CountryCode")),
        _normalize_text(event.get("ActionGeo_Lat")),
        _normalize_text(event.get("ActionGeo_Long")),
        _normalize_text(event.get("AvgTone")),
    ]
    digest = hashlib.sha256("|".join(composite_parts).encode("utf-8")).hexdigest()
    return f"gdelt-composite:{digest}"


def normalize_event_for_insert(
    event: dict[str, Any],
    *,
    export_time_utc: datetime,
    export_url: str,
    ingestion_run_id: UUID,
) -> dict[str, Any]:
    """Map a parsed GDELT row to the raw_events table shape."""

    sql_date = _parse_sql_date(event.get("SQLDATE"))
    date_added_utc = _parse_date_added(event.get("DATEADDED"))
    # prefer DATEADDED when available, then SQLDATE-at-midnight, then export discovery time
    event_time_utc = date_added_utc
    if event_time_utc is None and sql_date is not None:
        event_time_utc = datetime.combine(sql_date, time.min, tzinfo=timezone.utc)
    if event_time_utc is None:
        event_time_utc = export_time_utc
    source_url = _clean_text(event.get("SOURCEURL"))
    category_result = categorize_event(
        {
            "event_code": _clean_text(event.get("EventCode")),
            "actor1_name": _clean_text(event.get("Actor1Name")),
            "actor2_name": _clean_text(event.get("Actor2Name")),
            "action_geo_full_name": _clean_text(event.get("ActionGeo_FullName")),
            "source_url": source_url,
        }
    )

    return {
        "source": SOURCE,
        "export_time_utc": export_time_utc,
        "export_url": export_url,
        "ingestion_run_id": ingestion_run_id,
        "dedupe_key": build_dedupe_key(event),
        "global_event_id": _parse_int(event.get("GLOBALEVENTID")),
        "sql_date": sql_date,
        "event_time_utc": event_time_utc,
        "actor1_name": _clean_text(event.get("Actor1Name")),
        "actor2_name": _clean_text(event.get("Actor2Name")),
        "event_code": _clean_text(event.get("EventCode")),
        "action_geo_full_name": _clean_text(event.get("ActionGeo_FullName")),
        "action_geo_country_code": _clean_text(event.get("ActionGeo_CountryCode")),
        "action_geo_lat": _parse_decimal(event.get("ActionGeo_Lat")),
        "action_geo_long": _parse_decimal(event.get("ActionGeo_Long")),
        "avg_tone": _parse_decimal(event.get("AvgTone")),
        "goldstein_score": _parse_decimal(event.get("GoldsteinScale")),
        "source_url": source_url,
        "primary_category": category_result.primary_category,
        "secondary_category": category_result.secondary_category,
        "category_confidence": Decimal(str(category_result.category_confidence)),
        "category_reason": category_result.category_reason,
        "raw_payload": dict(event),
    }
