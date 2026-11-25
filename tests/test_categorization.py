import pytest

from src.domain.events.categorization import categorize_event


@pytest.mark.parametrize("code, category", [("176", "politics"), ("155", "politics"), ("064", "economics")])
def test_unsupported_or_generic_codes_do_not_establish_cyber(code, category):
    assert categorize_event({"event_code": code}).primary_category == category


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


@pytest.mark.parametrize("code", ["023", "024", "025"])
def test_appeal_only_changes_category_with_humanitarian_context(code):
    assert categorize_event({"event_code": code}).primary_category == "diplomacy"
    relevant = categorize_event({"event_code": code, "source_url": "https://example.net/emergency-aid-convoy"})
    assert (relevant.primary_category, relevant.secondary_category) == ("crisis", "humanitarian")


@pytest.mark.parametrize("url", [
    "https://example.org/council-breaches-spending-rules", "https://ransomware.example.org/company-update",
    "https://example.org/company-update?ref=malware", "https://example.org/company-update#data-breach",
    "https://example.org/geodata-breach-notice", "https://example.org/election-landslide",
    "https://example.org/player-displaced-from-team", "https://example.org/outbreak-of-violence",
    "https://example.org/security-breach-at-embassy",
])
def test_broad_words_or_non_article_url_fields_do_not_override(url):
    assert categorize_event({"event_code": "111", "source_url": url}).primary_category == "politics"


@pytest.mark.parametrize("slug", ["data%20breaches", "network-breach", "suspected-cyber-intrusion"])
def test_specific_cyber_phrase_can_override_generic_code(slug):
    assert categorize_event({"event_code": "064", "source_url": f"https://example.org/{slug}"}).primary_category == "cyber"


def test_floodwaters_can_supply_disaster_context():
    result = categorize_event({"event_code": "073", "source_url": "https://example.org/residents-evacuated-from-floodwaters"})
    assert (result.primary_category, result.secondary_category) == ("crisis", "natural_disaster")


def test_military_posture_and_protest_keep_distinct_categories():
    assert categorize_event({"event_code": "154"}).primary_category == "conflict"
    assert categorize_event({"event_code": "143"}).primary_category == "protest"


def test_categorize_event_falls_back_to_root_category():
    result = categorize_event({"event_code": "190"})

    assert result.primary_category == "conflict"
    assert result.secondary_category is None
