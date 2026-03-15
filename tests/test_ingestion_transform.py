from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from src.ingestion.transform import build_dedupe_key, normalize_event_for_insert


def test_build_dedupe_key_prefers_global_event_id():
    event = {"GLOBALEVENTID": "12345"}

    assert build_dedupe_key(event) == "gdelt-global-event-id:12345"


def test_build_dedupe_key_falls_back_to_stable_composite():
    first = {
        "SQLDATE": "20260313",
        "Actor1Name": " Police ",
        "Actor2Name": "PROTESTERS",
        "EventCode": "190",
        "ActionGeo_FullName": "Washington, DC",
        "ActionGeo_CountryCode": "USA",
        "ActionGeo_Lat": "38.9072",
        "ActionGeo_Long": "-77.0369",
        "AvgTone": "-2.5",
    }
    second = {
        "SQLDATE": "20260313",
        "Actor1Name": "police",
        "Actor2Name": " protesters ",
        "EventCode": "190",
        "ActionGeo_FullName": " Washington, DC ",
        "ActionGeo_CountryCode": "usa",
        "ActionGeo_Lat": "38.9072",
        "ActionGeo_Long": "-77.0369",
        "AvgTone": "-2.5",
    }

    assert build_dedupe_key(first) == build_dedupe_key(second)


def test_normalize_event_for_insert_maps_expected_fields():
    normalized = normalize_event_for_insert(
        {
            "GLOBALEVENTID": "12345",
            "SQLDATE": "20260313",
            "Actor1Name": "POLICE",
            "Actor2Name": "PROTESTERS",
            "EventCode": "190",
            "ActionGeo_FullName": "Washington, DC",
            "ActionGeo_CountryCode": "USA",
            "ActionGeo_Lat": "38.9072",
            "ActionGeo_Long": "-77.0369",
            "AvgTone": "-2.5",
            "GoldsteinScale": "-5.0",
            "SOURCEURL": "https://example.com/cyber-attack-report",
        },
        export_time_utc=datetime(2026, 3, 13, 0, 15, tzinfo=timezone.utc),
        export_url="http://data.gdeltproject.org/gdeltv2/20260313001500.export.CSV.zip",
        ingestion_run_id=uuid4(),
    )

    assert normalized["global_event_id"] == 12345
    assert normalized["sql_date"].isoformat() == "2026-03-13"
    assert normalized["action_geo_lat"] == Decimal("38.9072")
    assert normalized["action_geo_long"] == Decimal("-77.0369")
    assert normalized["avg_tone"] == Decimal("-2.5")
    assert normalized["goldstein_score"] == Decimal("-5.0")
    assert normalized["source_url"] == "https://example.com/cyber-attack-report"
    assert normalized["event_time_utc"].isoformat() == "2026-03-13T00:00:00+00:00"
    assert normalized["primary_category"] == "cyber"
    assert normalized["category_confidence"] > Decimal("0.8")
