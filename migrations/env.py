from __future__ import annotations

from logging.config import fileConfig
import os
from pathlib import Path

from alembic import context
from sqlalchemy import create_engine, pool
from sqlalchemy.engine import make_url

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

if load_dotenv is not None:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")

database_url = make_url(
    config.attributes.get("database_url")
    or os.getenv("DATABASE_URL")
    or config.get_main_option("sqlalchemy.url")
)
if database_url.drivername == "postgresql":
    database_url = database_url.set(drivername="postgresql+psycopg")

target_metadata = None


def run_migrations_offline() -> None:
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = create_engine(
        database_url,
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
