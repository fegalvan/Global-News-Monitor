"""analytics spike snapshot table

Revision ID: 0004_analytics_spike_snapshot
Revises: 0003_stage3_indexes
Create Date: 2026-03-21 00:00:00
"""

from __future__ import annotations

from pathlib import Path

from alembic import op

revision = "0004_analytics_spike_snapshot"
down_revision = "0003_stage3_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    root = Path(__file__).resolve().parents[2]
    stage4_sql = (root / "sql" / "stage4_analytics_snapshot.sql").read_text(encoding="utf-8")
    op.execute(stage4_sql)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS analytics_spike_snapshot")
