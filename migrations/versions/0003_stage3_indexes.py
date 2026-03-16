"""stage3 analytics indexes

Revision ID: 0003_stage3_indexes
Revises: 0002_stage2_schema
Create Date: 2026-03-16 00:02:00
"""

from __future__ import annotations

from pathlib import Path

from alembic import op

revision = "0003_stage3_indexes"
down_revision = "0002_stage2_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    root = Path(__file__).resolve().parents[2]
    stage3_sql = (root / "sql" / "stage3_indexes.sql").read_text(encoding="utf-8")
    op.execute(stage3_sql)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_events_location")
    op.execute("DROP INDEX IF EXISTS idx_events_category_country_time")
    op.execute("DROP INDEX IF EXISTS idx_events_country_time")
    op.execute("DROP INDEX IF EXISTS idx_events_time")
