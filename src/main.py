from src.gdelt_events import fetch_latest_events

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

EVENT_CATEGORIES = {
    "01": "diplomacy",
    "02": "diplomacy",
    "03": "diplomacy",
    "04": "diplomacy",
    "05": "diplomacy",
    "06": "economics",
    "07": "economics",
    "08": "economics",
    "09": "politics",
    "10": "politics",
    "11": "politics",
    "12": "politics",
    "14": "protest",
    "15": "protest",
    "17": "conflict",
    "18": "conflict",
    "19": "conflict",
    "20": "conflict",
}

TECH_EVENT_CODES = {"155", "176", "064"}
CRISIS_EVENT_CODES = {"023", "024", "025"}
MAX_EVENTS_PER_CATEGORY = 3
MAX_BREAKING_EVENTS = 5


def get_event_category(event_code: str) -> str:
    if event_code in TECH_EVENT_CODES:
        return "tech"

    if event_code in CRISIS_EVENT_CODES:
        return "crisis"

    prefix = event_code[:2]
    return EVENT_CATEGORIES.get(prefix, "other")


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

    print(f"{actor1} \u2192 {actor2} | {event_label} ({event_code}) | {country} | {lat}, {lon}")


def main():
    print("Global News Monitor starting...")

    events = fetch_latest_events()
    grouped_events = {
        "diplomacy": [],
        "economics": [],
        "politics": [],
        "protest": [],
        "conflict": [],
        "tech": [],
        "crisis": [],
    }

    high_tension_events = []

    for event in events:
        event_code = event.get("EventCode") or ""
        category = get_event_category(event_code)
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


if __name__ == "__main__":
    main()
