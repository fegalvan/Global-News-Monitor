from fastapi.testclient import TestClient

from src.api.main import app


def test_health_endpoint_returns_ok():
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_latest_endpoint_returns_json(monkeypatch):
    client = TestClient(app)
    monkeypatch.setattr(
        "src.api.main.get_latest_payload",
        lambda limit: {"limit": limit, "rows": [{"event_code": "190", "country_name": "United States"}]},
    )

    response = client.get("/latest", params={"limit": 5})

    assert response.status_code == 200
    assert response.json()["limit"] == 5
    assert response.json()["rows"][0]["country_name"] == "United States"


def test_stats_endpoint_returns_json(monkeypatch):
    client = TestClient(app)
    monkeypatch.setattr(
        "src.api.main.get_stats_payload",
        lambda hours: {
            "hours": hours,
            "overview": {"total_events": 10, "unknown_country_count": 2},
            "category_counts": [],
            "top_countries": [{"country_name": "United States", "count": 4}],
            "event_code_counts": [],
        },
    )

    response = client.get("/stats", params={"hours": 12})

    assert response.status_code == 200
    assert response.json()["overview"]["unknown_country_count"] == 2


def test_spikes_and_tension_endpoints_return_json(monkeypatch):
    client = TestClient(app)
    monkeypatch.setattr(
        "src.api.main.get_spikes_payload",
        lambda hours: {"hours": hours, "rows": [{"category": "conflict", "country_name": "Ukraine"}]},
    )
    monkeypatch.setattr(
        "src.api.main.get_tension_payload",
        lambda hours: {"hours": hours, "rows": [{"actor1": "A", "actor2": "B", "category": "conflict"}]},
    )

    spikes_response = client.get("/spikes")
    tension_response = client.get("/tension")

    assert spikes_response.status_code == 200
    assert spikes_response.json()["rows"][0]["country_name"] == "Ukraine"
    assert tension_response.status_code == 200
    assert tension_response.json()["rows"][0]["category"] == "conflict"
