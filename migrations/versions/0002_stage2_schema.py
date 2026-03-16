"""stage2 schema

Revision ID: 0002_stage2_schema
Revises: 0001_stage1_schema
Create Date: 2026-03-16 00:01:00
"""

from __future__ import annotations

from pathlib import Path

from alembic import op

revision = "0002_stage2_schema"
down_revision = "0001_stage1_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    root = Path(__file__).resolve().parents[2]
    stage2_sql = (root / "sql" / "stage2_schema.sql").read_text(encoding="utf-8")
    op.execute(stage2_sql)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS normalized_events")
