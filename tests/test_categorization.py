from src.domain.events.categorization import categorize_event


def test_categorize_event_detects_cyber_from_strong_event_code():
    result = categorize_event({"event_code": "176"})

    assert result.primary_category == "cyber"
    assert result.secondary_category is None
    assert "cyber_strong_event_code" in result.category_reason


def test_categorize_event_detects_cyber_from_keywords():
    result = categorize_event(
        {
            "event_code": "112",
            "actor1_name": "National Cyber Defense Agency",
            "actor2_name": "Unknown hacking group",
            "source_url": "https://example.com/ransomware-attack",
        }
    )

    assert result.primary_category == "cyber"
    assert result.category_confidence >= 0.85


def test_categorize_event_detects_crisis_subcategory_epidemic():
    result = categorize_event(
        {
            "event_code": "112",
            "actor1_name": "Health Ministry",
            "actor2_name": "Hospital Network",
            "source_url": "https://example.com/cholera-outbreak-response",
        }
    )

    assert result.primary_category == "crisis"
    assert result.secondary_category == "epidemic"


def test_categorize_event_uses_weak_humanitarian_signal_when_only_code_exists():
    result = categorize_event({"event_code": "023"})

    assert result.primary_category == "crisis"
    assert result.secondary_category == "humanitarian"
    assert result.category_confidence < 0.7


def test_categorize_event_falls_back_to_root_category():
    result = categorize_event({"event_code": "190"})

    assert result.primary_category == "conflict"
    assert result.secondary_category is None
