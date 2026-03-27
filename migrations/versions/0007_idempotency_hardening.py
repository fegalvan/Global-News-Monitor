"""idempotency hardening for checkpoints and raw event dedupe

Revision ID: 0007_idempotency_hardening
Revises: 0006_ingestion_observability
Create Date: 2026-03-27 00:00:00
"""

from __future__ import annotations

from pathlib import Path

from alembic import op

revision = "0007_idempotency_hardening"
down_revision = "0006_ingestion_observability"
branch_labels = None
depends_on = None


def upgrade() -> None:
    root = Path(__file__).resolve().parents[2]
    stage7_sql = (root / "sql" / "stage7_idempotency_hardening.sql").read_text(encoding="utf-8")
    op.execute(stage7_sql)


def downgrade() -> None:
    # Keep the indexes on downgrade because they are safety-critical for idempotency.
    pass
