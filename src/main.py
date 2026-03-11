from src.gdelt_events import fetch_latest_events

EVENT_CODE_LABELS = {
    "017": "Appeal",
    "030": "Express intent to cooperate",
    "040": "Consult",
    "042": "Make a visit",
    "043": "Host a visit",
    "064": "Share intelligence or information",
    "084": "Release or return person/property",
    "145": "Violent protest",
    "155": "Mobilize cyber forces",
    "172": "Military attack",
    "176": "Cyber attack",
    "190": "Use conventional force",
    "193": "Fight with small arms",
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


def get_event_category(event_code: str) -> str:
    if event_code in TECH_EVENT_CODES:
        return "tech"

    if event_code in CRISIS_EVENT_CODES:
        return "crisis"

    prefix = event_code[:2]
    return EVENT_CATEGORIES.get(prefix, "other")


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

    for event in events:
        event_code = event.get("EventCode") or "Unknown"
        category = get_event_category(event_code)

        if category not in grouped_events:
            continue

        if len(grouped_events[category]) >= MAX_EVENTS_PER_CATEGORY:
            continue

        grouped_events[category].append(event)

    for category, category_events in grouped_events.items():
        if not category_events:
            continue

        print(f"\n[{category.upper()}]")

        for event in category_events:
            actor1 = event.get("Actor1Name") or "Unknown"
            actor2 = event.get("Actor2Name") or "Unknown"
            event_code = event.get("EventCode") or "Unknown"
            event_label = EVENT_CODE_LABELS.get(event_code, "Unknown event")
            print(f"{actor1} \u2192 {actor2} | {event_label} ({event_code})")


if __name__ == "__main__":
    main()
