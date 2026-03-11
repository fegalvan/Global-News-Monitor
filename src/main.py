from src.gdelt_events import fetch_latest_events

def main():
    print("Global News Monitor starting...")

    events = fetch_latest_events()

    for event in events[:5]:
        actor1 = event.get("Actor1Name") or "Unknown"
        actor2 = event.get("Actor2Name") or "Unknown"
        event_code = event.get("EventCode") or "Unknown"
        country = event.get("ActionGeo_CountryCode") or "Unknown"
        print(f"[EVENT] {actor1} → {actor2} | {event_code} | {country}")

if __name__ == "__main__":
    main()
    