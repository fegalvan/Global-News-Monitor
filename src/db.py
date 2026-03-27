"""Database helpers for PostgreSQL-backed ingestion."""

from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None

import psycopg
from psycopg.rows import dict_row


def load_database_url() -> str:
    """Load DATABASE_URL from the environment, with optional .env support."""

    if load_dotenv is not None:
        load_dotenv()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. Configure it in your environment before running ingestion."
        )

    return database_url


def get_connection() -> psycopg.Connection:
    """Create a PostgreSQL connection for the current process."""

    # autocommit=True is important here because we manage explicit transaction
    # boundaries with connection.transaction(). Without autocommit, psycopg
    # keeps an outer implicit transaction open and closing the connection can
    # roll back all writes.
    return psycopg.connect(
        load_database_url(),
        row_factory=dict_row,
        autocommit=True,
    )


@contextmanager
def transaction(connection: psycopg.Connection) -> Iterator[psycopg.Connection]:
    """Wrap a series of SQL statements in a database transaction."""

    with connection.transaction():
        yield connection


def fetch_one(
    connection: psycopg.Connection,
    query: str,
    params: Sequence[Any] | None = None,
) -> dict[str, Any] | None:
    """Execute a query and return one row as a dictionary."""

    with connection.cursor() as cursor:
        cursor.execute(query, params or ())
        return cursor.fetchone()


def fetch_all(
    connection: psycopg.Connection,
    query: str,
    params: Sequence[Any] | None = None,
) -> list[dict[str, Any]]:
    """Execute a query and return all rows as dictionaries."""

    with connection.cursor() as cursor:
        cursor.execute(query, params or ())
        return list(cursor.fetchall())


def execute(
    connection: psycopg.Connection,
    query: str,
    params: Sequence[Any] | None = None,
) -> None:
    """Execute a statement that does not need to return rows."""

    with connection.cursor() as cursor:
        cursor.execute(query, params or ())
