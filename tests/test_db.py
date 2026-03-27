from src import db


def test_get_connection_enables_autocommit(monkeypatch):
    captured = {}

    class FakeConnection:
        pass

    def fake_connect(dsn, row_factory=None, autocommit=None):
        captured["dsn"] = dsn
        captured["row_factory"] = row_factory
        captured["autocommit"] = autocommit
        return FakeConnection()

    monkeypatch.setattr("src.db.load_database_url", lambda: "postgresql://example")
    monkeypatch.setattr("src.db.psycopg.connect", fake_connect)

    connection = db.get_connection()

    assert isinstance(connection, FakeConnection)
    assert captured["autocommit"] is True
