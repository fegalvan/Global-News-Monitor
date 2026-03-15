from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from typing import Any, Mapping


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
    "15": PrimaryCategory.PROTEST.value,
    "16": PrimaryCategory.DIPLOMACY.value,
    "17": PrimaryCategory.CONFLICT.value,
    "18": PrimaryCategory.CONFLICT.value,
    "19": PrimaryCategory.CONFLICT.value,
    "20": PrimaryCategory.CONFLICT.value,
}

CYBER_EVENT_CODES_STRONG = {"176", "155"}
CYBER_EVENT_CODES_MEDIUM = {"064"}

# these are quick keyword sets so cyber/crisis get treated like real top-level categories
CYBER_KEYWORDS = {
    "cyber attack",
    "cyberattack",
    "cyber warfare",
    "cyberwarfare",
    "hacking",
    "hacker",
    "ransomware",
    "malware",
    "data breach",
    "breach",
    "ddos",
    "phishing",
}

CRISIS_KEYWORDS = {
    "environmental": {
        "environmental disaster",
        "pollution",
        "toxic spill",
        "heatwave",
        "drought",
        "water shortage",
        "air quality emergency",
    },
    "humanitarian": {
        "humanitarian crisis",
        "refugee",
        "displaced",
        "internally displaced",
        "aid convoy",
        "famine",
        "food insecurity",
        "relief operation",
    },
    "epidemic": {
        "epidemic",
        "outbreak",
        "pandemic",
        "public health emergency",
        "cholera",
        "ebola",
        "avian flu",
        "covid",
    },
    "natural_disaster": {
        "earthquake",
        "hurricane",
        "cyclone",
        "typhoon",
        "tsunami",
        "volcano",
        "wildfire",
        "flood",
        "landslide",
    },
}

WEAK_HUMANITARIAN_EVENT_CODES = {"023", "024", "025"}
NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")


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


def _build_text_blob(event: Mapping[str, Any]) -> str:
    # im collecting a cheap text blob so the keyword rules are deterministic and easy to debug
    parts = [
        _get_field(event, "actor1_name", "Actor1Name"),
        _get_field(event, "actor2_name", "Actor2Name"),
        _get_field(event, "action_geo_full_name", "ActionGeo_FullName"),
        _get_field(event, "source_url", "SOURCEURL"),
    ]
    # replacing punctuation keeps phrases like "cyber-attack" discoverable by plain keyword rules
    return NON_ALNUM_PATTERN.sub(" ", " ".join(parts).casefold())


def _detect_cyber(event_code: str, text_blob: str) -> CategoryResult | None:
    if event_code in CYBER_EVENT_CODES_STRONG:
        return CategoryResult(
            primary_category=PrimaryCategory.CYBER.value,
            secondary_category=None,
            category_confidence=0.95,
            category_reason=f"cyber_strong_event_code:{event_code}",
        )

    keyword_hits = [keyword for keyword in CYBER_KEYWORDS if keyword in text_blob]
    if keyword_hits:
        return CategoryResult(
            primary_category=PrimaryCategory.CYBER.value,
            secondary_category=None,
            category_confidence=0.9,
            category_reason=f"cyber_keyword:{keyword_hits[0]}",
        )

    if event_code in CYBER_EVENT_CODES_MEDIUM:
        return CategoryResult(
            primary_category=PrimaryCategory.CYBER.value,
            secondary_category=None,
            category_confidence=0.72,
            category_reason=f"cyber_medium_event_code:{event_code}",
        )

    return None


def _detect_crisis(event_code: str, text_blob: str) -> CategoryResult | None:
    for subcategory, keywords in CRISIS_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text_blob:
                confidence = 0.88
                if subcategory in {"epidemic", "natural_disaster"}:
                    confidence = 0.92
                return CategoryResult(
                    primary_category=PrimaryCategory.CRISIS.value,
                    secondary_category=subcategory,
                    category_confidence=confidence,
                    category_reason=f"crisis_keyword:{subcategory}:{keyword}",
                )

    if event_code in WEAK_HUMANITARIAN_EVENT_CODES:
        # these codes are weak signals only, not enough to replace stronger crisis rules above
        return CategoryResult(
            primary_category=PrimaryCategory.CRISIS.value,
            secondary_category="humanitarian",
            category_confidence=0.56,
            category_reason=f"crisis_weak_humanitarian_code:{event_code}",
        )

    return None


def categorize_event(event: Mapping[str, Any]) -> CategoryResult:
    event_code = _get_field(event, "event_code", "EventCode")
    event_root = event_code[:2]
    text_blob = _build_text_blob(event)

    cyber_match = _detect_cyber(event_code, text_blob)
    if cyber_match is not None:
        return cyber_match

    crisis_match = _detect_crisis(event_code, text_blob)
    if crisis_match is not None:
        return crisis_match

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
