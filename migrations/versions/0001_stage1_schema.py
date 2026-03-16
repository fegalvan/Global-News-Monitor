"""stage1 schema

Revision ID: 0001_stage1_schema
Revises:
Create Date: 2026-03-16 00:00:00
"""

from __future__ import annotations

from pathlib import Path

from alembic import op

revision = "0001_stage1_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    root = Path(__file__).resolve().parents[2]
    stage1_sql = (root / "sql" / "stage1_schema.sql").read_text(encoding="utf-8")
    op.execute(stage1_sql)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS normalized_events")
    op.execute("DROP TABLE IF EXISTS raw_events")
    op.execute("DROP TABLE IF EXISTS gdelt_export_checkpoints")
    op.execute("DROP TABLE IF EXISTS ingestion_runs")

