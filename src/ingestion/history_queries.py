"""Whole-hour history with explicit export coverage."""

from datetime import datetime, timedelta, timezone
from math import sqrt
from statistics import median

from src.db import fetch_all, fetch_one

MIN_PERCENT_BASELINE = 20


def fetch_periods(connection, windows, country: str | None, category: str | None):
    conditions = ["h.historical_count > 0"]
    params = [[start for start, _ in windows], [end for _, end in windows]]
    if country:
        conditions.append("h.country_code = %s")
        params.append(country)
    if category:
        conditions.append("h.primary_category = %s")
        params.append(category)
    return fetch_all(connection, f"""
        WITH windows AS (
            SELECT * FROM unnest(%s::timestamptz[], %s::timestamptz[])
                WITH ORDINALITY AS w(start, "end", ordinal)
        )
        SELECT w.start, w.end,
               (extract(epoch FROM w.end - w.start) / 900)::int AS expected,
               (SELECT count(*) FROM event_history_coverage c
                WHERE c.export_time_utc >= w.start AND c.export_time_utc < w.end
                  AND mod(extract(epoch FROM c.export_time_utc), 900) = 0) AS completed,
               (SELECT coalesce(sum(h.historical_count), 0)::bigint FROM event_hourly_counts h
                WHERE h.hour_utc >= w.start AND h.hour_utc < w.end AND {' AND '.join(conditions)}) AS observed
        FROM windows w
        ORDER BY ordinal
    """, params)


def full_observation(period: dict) -> int | None:
    return period["observed"] if period["completed"] == period["expected"] else None


def compare_periods(current: dict, previous: dict) -> dict:
    current_count, previous_count = full_observation(current), full_observation(previous)
    change = None if current_count is None or previous_count is None else current_count - previous_count
    return {
        "current": current,
        "previous": previous,
        "absolute_change": change,
        "percent_change": (
            100 * change / previous_count if change is not None and previous_count >= MIN_PERCENT_BASELINE else None
        ),
    }


def fetch_category_changes(connection, comparison: dict, country: str | None) -> list[dict]:
    if comparison["absolute_change"] is None:
        return []
    current, previous = comparison["current"], comparison["previous"]
    country_clause = " AND country_code = %s" if country else ""
    split = current["start"]
    params = [split, split, previous["start"], current["end"]]
    if country:
        params.append(country)
    return fetch_all(connection, f"""
        WITH counts AS (
            SELECT primary_category AS category,
                   coalesce(sum(historical_count) FILTER (WHERE hour_utc >= %s), 0)::bigint AS current,
                   coalesce(sum(historical_count) FILTER (WHERE hour_utc < %s), 0)::bigint AS previous
            FROM event_hourly_counts
            WHERE hour_utc >= %s AND hour_utc < %s AND historical_count > 0{country_clause}
            GROUP BY primary_category
        )
        SELECT *, current - previous AS change
        FROM counts ORDER BY abs(current - previous) DESC, category
    """, params)


def reporting_anomaly(current: dict, baseline: list[dict]) -> dict:
    observed = full_observation(current)
    samples = [count for period in baseline if (count := full_observation(period)) is not None]
    result = {
        "status": "insufficient_history", "baseline_samples": len(samples), "minimum_samples": 8,
        "observed": observed, "expected": None, "robust_scale": None, "score": None,
    }
    if observed is None:
        return {**result, "status": "incomplete_observation"}
    if len(samples) < 8:
        return result
    expected = median(samples)
    if expected < 20:
        return {**result, "status": "low_baseline_volume", "expected": expected}
    mad = median(abs(sample - expected) for sample in samples)
    scale = max(1.4826 * mad, sqrt(expected))
    excess = observed - expected
    score = excess / scale
    elevated = score >= 3.5 and excess >= 20 and excess >= expected * 0.5
    return {
        **result, "status": "elevated" if elevated else "within_threshold",
        "expected": expected, "robust_scale": scale, "score": score,
    }


def fetch_history(connection, end: datetime, country: str | None, category: str | None) -> dict:
    end = end.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    boundary = fetch_one(connection, """
        SELECT corrected_from FROM event_category_revision WHERE rule_version = 'v1.2'
    """)["corrected_from"]

    def comparable(period: dict) -> bool:
        return boundary is None or period["start"] >= boundary

    day, week = timedelta(days=1), timedelta(days=7)
    periods = [(end - day, end), (end - 2 * day, end - day), (end - week, end), (end - 2 * week, end - week)]
    periods += [(end - n * week - day, end - n * week) for n in range(1, 13)]
    results = fetch_periods(connection, periods, country, category)

    midnight = end.replace(hour=0)
    daily = fetch_periods(
        connection,
        [(midnight - n * day, midnight - (n - 1) * day) for n in range(36, 0, -1)],
        country, category,
    )
    for row in daily:
        row["count"] = full_observation(row) if not category or comparable(row) else None
    for index in range(6, len(daily)):
        counts = [row["count"] for row in daily[index - 6:index + 1]]
        daily[index]["rolling_average"] = None if None in counts else sum(counts) / 7
    comparison_24h = compare_periods(results[0], results[1])
    comparison_7d = compare_periods(results[2], results[3])
    for comparison in (comparison_24h, comparison_7d):
        if not comparable(comparison["previous"]):
            comparison["category_rules_changed"] = True
            if category:
                comparison.update(absolute_change=None, percent_change=None)
        elif not category:
            comparison["category_changes"] = fetch_category_changes(connection, comparison, country)
    baseline = [period for period in results[4:] if not category or comparable(period)]
    anomaly = reporting_anomaly(results[0], baseline)
    if category and not comparable(results[0]):
        anomaly.update(status="category_rules_changed", observed=None, score=None)
    return {
        "comparison_24h": comparison_24h,
        "comparison_7d": comparison_7d,
        "anomaly": anomaly,
        "daily": daily[6:],
        "category_history_from": boundary,
    }
