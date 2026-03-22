from datetime import datetime, timezone
from decimal import Decimal

from src.ingestion.validation import validate_and_clean_event


def _event_with_country(country_code: str | None) -> dict:
    return {
        "event_time_utc": datetime(2026, 3, 21, 0, 0, tzinfo=timezone.utc),
        "event_code": "190",
        "primary_category": "conflict",
        "action_geo_country_code": country_code,
        "action_geo_lat": Decimal("10.0"),
        "action_geo_long": Decimal("20.0"),
        "avg_tone": Decimal("-1.0"),
        "actor1_name": "A",
        "actor2_name": "B",
        "raw_payload": {"ActionGeo_CountryCode": country_code},
    }


def test_validate_and_clean_event_preserves_two_letter_country_code():
    cleaned, flags, dropped = validate_and_clean_event(
        _event_with_country("us"),
        datetime(2026, 3, 21, 0, 15, tzinfo=timezone.utc),
    )

    assert dropped is False
    assert cleaned["action_geo_country_code"] == "US"
    assert "bad_country_code" not in flags


def test_validate_and_clean_event_preserves_three_letter_country_code():
    cleaned, flags, dropped = validate_and_clean_event(
        _event_with_country("usa"),
        datetime(2026, 3, 21, 0, 15, tzinfo=timezone.utc),
    )

    assert dropped is False
    assert cleaned["action_geo_country_code"] == "USA"
    assert "bad_country_code" not in flags


def test_validate_and_clean_event_maps_unknown_country_to_null():
    cleaned, flags, dropped = validate_and_clean_event(
        _event_with_country(" Unknown "),
        datetime(2026, 3, 21, 0, 15, tzinfo=timezone.utc),
    )

    assert dropped is False
    assert cleaned["action_geo_country_code"] is None
    assert "bad_country_code" in flags


def test_validate_and_clean_event_falls_back_to_raw_payload_country():
    event = _event_with_country(None)
    event["raw_payload"] = {"ActionGeo_CountryCode": "br"}

    cleaned, flags, dropped = validate_and_clean_event(
        event,
        datetime(2026, 3, 21, 0, 15, tzinfo=timezone.utc),
    )

    assert dropped is False
    assert cleaned["action_geo_country_code"] == "BR"
    assert "bad_country_code" not in flags


def test_validate_and_clean_event_preserves_zero_values_from_primary_fields():
    event = {
        "event_time_utc": datetime(2026, 3, 21, 0, 0, tzinfo=timezone.utc),
        "event_code": "190",
        "primary_category": "conflict",
        "country_code": "US",
        "latitude": Decimal("0"),
        "longitude": Decimal("0"),
        "tone": Decimal("0"),
        "actor1_name": "A",
        "actor2_name": "B",
        "raw_payload": {},
    }

    cleaned, flags, dropped = validate_and_clean_event(
        event,
        datetime(2026, 3, 21, 0, 15, tzinfo=timezone.utc),
    )

    assert dropped is False
    assert cleaned["action_geo_lat"] == Decimal("0")
    assert cleaned["action_geo_long"] == Decimal("0")
    assert cleaned["avg_tone"] == Decimal("0")
    assert "missing_tone" not in flags
