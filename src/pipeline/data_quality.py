from __future__ import annotations

from collections.abc import Sequence
from typing import Any


def summarize_batch_quality(events: Sequence[dict[str, Any]]) -> dict[str, Any]:
    # quick stats so ingestion can warn us when data quality starts drifting
    missing_actor_count = 0
    missing_geo_count = 0
    unknown_country_count = 0
    category_counts: dict[str, int] = {}

    for event in events:
        actor1 = event.get("actor1_name")
        actor2 = event.get("actor2_name")
        if not actor1 and not actor2:
            missing_actor_count += 1

        lat = event.get("action_geo_lat")
        lon = event.get("action_geo_long")
        country = event.get("action_geo_country_code")
        country_name = event.get("country_name")
        if not country and lat is None and lon is None:
            missing_geo_count += 1
        if not country or country_name == "Unknown":
            unknown_country_count += 1

        category = str(event.get("primary_category") or "unknown")
        category_counts[category] = category_counts.get(category, 0) + 1

    return {
        "missing_actor_count": missing_actor_count,
        "missing_geo_count": missing_geo_count,
        "unknown_country_count": unknown_country_count,
        "category_counts": category_counts,
    }
