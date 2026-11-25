from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from typing import Any, Mapping
from urllib.parse import unquote, urlsplit

from src.domain.events.reference import normalize_event_code


class PrimaryCategory(str, Enum):
    CONFLICT = "conflict"
    PROTEST = "protest"
    POLITICS = "politics"
    DIPLOMACY = "diplomacy"
    ECONOMICS = "economics"
    CYBER = "cyber"
    CRISIS = "crisis"


CRISIS_SUBCATEGORIES = {
    "environmental",
    "humanitarian",
    "epidemic",
    "natural_disaster",
}

ROOT_CATEGORY_MAP = {
    "01": PrimaryCategory.DIPLOMACY.value,
    "02": PrimaryCategory.DIPLOMACY.value,
    "03": PrimaryCategory.DIPLOMACY.value,
    "04": PrimaryCategory.DIPLOMACY.value,
    "05": PrimaryCategory.DIPLOMACY.value,
    "06": PrimaryCategory.ECONOMICS.value,
    "07": PrimaryCategory.ECONOMICS.value,
    "08": PrimaryCategory.ECONOMICS.value,
    "09": PrimaryCategory.POLITICS.value,
    "10": PrimaryCategory.POLITICS.value,
    "11": PrimaryCategory.POLITICS.value,
    "12": PrimaryCategory.POLITICS.value,
    "13": PrimaryCategory.POLITICS.value,
    "14": PrimaryCategory.PROTEST.value,
    "15": PrimaryCategory.CONFLICT.value,  # CAMEO military posture
    "16": PrimaryCategory.DIPLOMACY.value,
    "17": PrimaryCategory.CONFLICT.value,
    "18": PrimaryCategory.CONFLICT.value,
    "19": PrimaryCategory.CONFLICT.value,
    "20": PrimaryCategory.CONFLICT.value,
}

NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")


def _terms(pattern: str) -> re.Pattern:
    return re.compile(rf"\b(?:{pattern})\b")


CYBER_TERMS = _terms(
    r"cyber ?attacks?|cyber ?incidents?|cyber ?warfare|cyber ?espionage|(?:data|network) breach(?:es)?"
    r"|(?:network|cyber|computer) intrusions?|ransomware|malware|ddos|phishing|default passwords?"
    r"|computer hacking|hacking groups?|hacking risks?|(?:networks?|accounts?|databases?) hack(?:s|ed)"
)
CRISIS_TERMS = {
    "environmental": _terms(
        r"environmental disaster|toxic spills?|oil spills?|heat ?waves?|severe drought|drought hit"
        r"|drought relief|water shortages?|air quality emergency|landfill (?:fires?|emergency)"
    ),
    "humanitarian": _terms(
        r"humanitarian (?:crisis|emergency|aid)|refugee (?:crisis|camps?|families)|forcibly displaced"
        r"|internally displaced|displaced (?:families|people)|aid convoys?|famine|food insecurity"
    ),
    "epidemic": _terms(
        r"public health emergency|cholera|ebola|avian flu|bird flu|disease outbreak|measles outbreak"
        r"|covid(?: 19)? (?:outbreak|cases|surge)"
    ),
    "natural_disaster": _terms(
        r"earthquakes?|hurricanes?|cyclones?|typhoons?|tsunamis?|volcanic eruption|wildfires?"
        r"|flooding|flood ?waters?|flash floods?|flood (?:relief|hit|warning|communities)|deadly floods?|floods? (?:kills?|hit)"
        r"|deadly landslides?|landslides? (?:kills?|hit|warning|debris)|mudslides?"
    ),
}


@dataclass(frozen=True)
class CategoryResult:
    primary_category: str
    secondary_category: str | None
    category_confidence: float
    category_reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "primary_category": self.primary_category,
            "secondary_category": self.secondary_category,
            "category_confidence": self.category_confidence,
            "category_reason": self.category_reason,
        }


def _get_field(event: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = event.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _context_fields(event: Mapping[str, Any]) -> tuple[str, ...]:
    source = _get_field(event, "source_url", "SOURCEURL")
    try:
        path = unquote(urlsplit(source).path)
    except ValueError:
        path = ""
    fields = (
        _get_field(event, "actor1_name", "Actor1Name"),
        _get_field(event, "actor2_name", "Actor2Name"),
        _get_field(event, "action_geo_full_name", "ActionGeo_FullName"),
        *path.split("/")[-3:],
    )
    return tuple(NON_ALNUM_PATTERN.sub(" ", value.casefold()) for value in fields if value)


def _match(pattern: re.Pattern, fields: tuple[str, ...]) -> str | None:
    for field in fields:
        if match := pattern.search(field):
            return match[0]
    return None


def categorize_event_v12(event: Mapping[str, Any]) -> CategoryResult:
    """The v1.2 release rules, also used by the retained-data migration."""
    event_code = normalize_event_code(_get_field(event, "event_code", "EventCode")) or ""
    event_root = event_code[:2]
    fields = _context_fields(event)
    if keyword := _match(CYBER_TERMS, fields):
        return CategoryResult(PrimaryCategory.CYBER.value, None, 0.9, f"cyber_keyword:{keyword}")

    for subcategory, pattern in CRISIS_TERMS.items():
        if keyword := _match(pattern, fields):
            confidence = 0.92 if subcategory in {"epidemic", "natural_disaster"} else 0.88
            return CategoryResult(
                PrimaryCategory.CRISIS.value, subcategory, confidence, f"crisis_keyword:{subcategory}:{keyword}",
            )

    base_category = ROOT_CATEGORY_MAP.get(event_root)
    if base_category is not None:
        return CategoryResult(
            primary_category=base_category,
            secondary_category=None,
            category_confidence=0.82,
            category_reason=f"root_event_code:{event_root}",
        )

    # fallback so callers always get one of the first-class categories
    return CategoryResult(
        primary_category=PrimaryCategory.POLITICS.value,
        secondary_category=None,
        category_confidence=0.35,
        category_reason="fallback_unknown_event_code",
    )


# Future rule revisions should keep categorize_event_v12 stable for migration replay.
categorize_event = categorize_event_v12
