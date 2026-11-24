from src.ingestion.history_queries import compare_periods, reporting_anomaly


def period(count, completed=96, expected=96):
    return {"observed": count, "completed": completed, "expected": expected}


def test_comparisons_distinguish_zero_and_missing():
    zero_baseline = compare_periods(period(10), period(0))
    assert zero_baseline["absolute_change"] == 10
    assert zero_baseline["percent_change"] is None
    assert compare_periods(period(10), period(0, 0))["absolute_change"] is None
    assert compare_periods(period(0), period(20))["percent_change"] == -100
    assert compare_periods(period(120), period(80))["percent_change"] == 50


def test_anomaly_uses_only_complete_comparable_observations():
    baseline = [period(count) for count in (40, 40, 40, 40, 40, 40, 40)]
    assert reporting_anomaly(period(80), baseline + [period(0, 0)])["status"] == "insufficient_history"
    assert reporting_anomaly(period(80, 95), baseline)["status"] == "incomplete_observation"
    result = reporting_anomaly(period(80), baseline + [period(4000)])
    assert (result["status"], result["baseline_samples"], result["expected"]) == ("elevated", 8, 40)
    assert result["robust_scale"] == 40**0.5


def test_anomaly_suppresses_sparse_and_small_changes():
    low = reporting_anomaly(period(120), [period(2)] * 12)
    assert low["status"] == "low_baseline_volume"
    assert low["score"] is None
    assert reporting_anomaly(period(29), [period(20)] * 8)["status"] == "within_threshold"
    assert reporting_anomaly(period(145), [period(100)] * 8)["status"] == "within_threshold"
    assert reporting_anomaly(period(100), [period(n) for n in range(20, 100, 10)])["score"] < 3.5
