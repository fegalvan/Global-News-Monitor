"""country mapping column and data quality audit table

Revision ID: 0005_country_mapping_and_quality_audit
Revises: 0004_analytics_spike_snapshot
Create Date: 2026-03-25 00:00:00
"""

from __future__ import annotations

from pathlib import Path

from alembic import op

from src.utils.country_mapping import COUNTRY_NAME_BY_CODE

revision = "0005_country_mapping_and_quality_audit"
down_revision = "0004_analytics_spike_snapshot"
branch_labels = None
depends_on = None


def _country_name_case_expression(column_name: str) -> str:
    clauses = []
    for code, country_name in sorted(COUNTRY_NAME_BY_CODE.items()):
        safe_name = country_name.replace("'", "''")
        clauses.append(f"WHEN UPPER(BTRIM({column_name})) = '{code}' THEN '{safe_name}'")

    return "CASE " + " ".join(clauses) + " ELSE 'Unknown' END"


def upgrade() -> None:
    root = Path(__file__).resolve().parents[2]
    stage5_sql = (root / "sql" / "stage5_country_mapping_and_quality.sql").read_text(encoding="utf-8")
    op.execute(stage5_sql)
    op.execute(
        f"""
        UPDATE normalized_events
        SET country_name = {_country_name_case_expression("country_code")}
        WHERE country_name = 'Unknown'
           OR BTRIM(country_name) = ''
           OR country_name IS NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS data_quality_audit")
    op.execute("DROP INDEX IF EXISTS idx_normalized_events_country_name")
    op.execute("ALTER TABLE normalized_events DROP COLUMN IF EXISTS country_name")
