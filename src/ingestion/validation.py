from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

ALLOWED_CATEGORIES = {
    "conflict",
    "protest",
    "politics",
    "diplomacy",
    "economics",
    "cyber",
    "crisis",
}
COUNTRY_CODE_RE = re.compile(r"^[A-Z]{2,3}$")
EVENT_CODE_RE = re.compile(r"^\d{2,3}$")
UNKNOWN_TEXT_VALUES = {"", "UNKNOWN", "NULL", "NONE", "N/A", "NA", "-"}


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def _clean_actor(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text or text.lower() in {"unknown", "null", "none", "-"}:
        return None
    return text


def _first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _normalize_country_code(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip().upper()
    if text in UNKNOWN_TEXT_VALUES:
        return None
    if not COUNTRY_CODE_RE.match(text):
        return None
    return text


def validate_and_clean_event(
    event: dict[str, Any],
    export_time_utc: datetime,
) -> tuple[dict[str, Any], list[str], bool]:
    flags: list[str] = []
    drop = False

    event_time = event.get("event_time_utc") or export_time_utc
    if not isinstance(event_time, datetime):
        event_time = export_time_utc
        flags.append("time_fallback_export_time")
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    if event_time > datetime.now(timezone.utc) + timedelta(hours=24):
        flags.append("time_future_outlier")
        drop = True

    event_code = (event.get("event_code") or "").strip()
    if event_code and not EVENT_CODE_RE.match(event_code):
        event_code = None
        flags.append("bad_event_code")

    category = (event.get("primary_category") or "").strip().lower()
    if category not in ALLOWED_CATEGORIES:
        category = "politics"
        flags.append("category_fallback")

    country = _normalize_country_code(event.get("action_geo_country_code"))
    if country is None:
        country = _normalize_country_code(event.get("country_code"))
    if country is None:
        raw_payload = event.get("raw_payload")
        if isinstance(raw_payload, dict):
            country = _normalize_country_code(raw_payload.get("ActionGeo_CountryCode"))
    if country is None and (
        event.get("action_geo_country_code") is not None
        or event.get("country_code") is not None
    ):
        flags.append("bad_country_code")

    lat = _to_decimal(_first_not_none(event.get("latitude"), event.get("action_geo_lat")))
    lon = _to_decimal(_first_not_none(event.get("longitude"), event.get("action_geo_long")))
    if (lat is None) ^ (lon is None):
        lat = None
        lon = None
        flags.append("partial_geo")
    if lat is not None and lon is not None:
        if lat < Decimal("-90") or lat > Decimal("90") or lon < Decimal("-180") or lon > Decimal("180"):
            lat = None
            lon = None
            flags.append("invalid_geo_range")

    tone = _to_decimal(_first_not_none(event.get("tone"), event.get("avg_tone")))
    if tone is None:
        flags.append("missing_tone")
    else:
        if tone < Decimal("-20"):
            tone = Decimal("-20")
            flags.append("tone_clamped_low")
        elif tone > Decimal("20"):
            tone = Decimal("20")
            flags.append("tone_clamped_high")

    cleaned = dict(event)
    cleaned.update(
        {
            "event_time_utc": event_time,
            "event_code": event_code,
            "primary_category": category,
            "action_geo_country_code": country,
            "action_geo_lat": lat,
            "action_geo_long": lon,
            "avg_tone": tone,
            "actor1_name": _clean_actor(_first_not_none(event.get("actor1"), event.get("actor1_name"))),
            "actor2_name": _clean_actor(_first_not_none(event.get("actor2"), event.get("actor2_name"))),
            "quality_flags": flags,
        }
    )

    return cleaned, flags, drop
