"""ingestion observability and dropped event audit trail

Revision ID: 0006_ingestion_observability
Revises: 0005_country_mapping_and_quality_audit
Create Date: 2026-03-26 00:00:00
"""

from __future__ import annotations

from pathlib import Path

from alembic import op

revision = "0006_ingestion_observability"
down_revision = "0005_country_mapping_and_quality_audit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    root = Path(__file__).resolve().parents[2]
    stage6_sql = (root / "sql" / "stage6_ingestion_observability.sql").read_text(encoding="utf-8")
    op.execute(stage6_sql)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_dropped_events_reason_created")
    op.execute("DROP INDEX IF EXISTS idx_dropped_events_run_created")
    op.execute("DROP TABLE IF EXISTS dropped_events")
    op.execute("ALTER TABLE raw_events DROP COLUMN IF EXISTS validation_flags")
