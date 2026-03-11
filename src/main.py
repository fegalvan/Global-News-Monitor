from src.gdelt_events import fetch_latest_events

EVENT_CODE_LABELS = {
    "043": "Diplomatic consultation",
    "084": "Police action",
    "145": "Protest",
    "190": "Use of force",
    "193": "Fight",
}


def main():
    print("Global News Monitor starting...")

    events = fetch_latest_events()

    for event in events[:5]:
        actor1 = event.get("Actor1Name") or "Unknown"
        actor2 = event.get("Actor2Name") or "Unknown"
        event_code = event.get("EventCode") or "Unknown"
        event_label = EVENT_CODE_LABELS.get(event_code, "Unknown event")
        country = event.get("ActionGeo_CountryCode") or "Unknown"
        print(f"[EVENT] {actor1} \u2192 {actor2} | {event_label} ({event_code}) | {country}")


if __name__ == "__main__":
    main()
