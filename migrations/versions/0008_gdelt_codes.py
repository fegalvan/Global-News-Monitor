"""Reconcile retained country and event codes against the GDELT dictionaries."""

from pathlib import Path

import sqlalchemy as sa
from alembic import op

from src.domain.events.reference import EVENT_LABELS
from src.utils.country_mapping import COUNTRY_NAME_BY_CODE, MISSING_COUNTRY_VALUES

revision = "0008_gdelt_codes"
down_revision = "0007_idempotency_hardening"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("LOCK TABLE raw_events, normalized_events IN SHARE ROW EXCLUSIVE MODE")
    op.create_table(
        "gdelt_geo_countries",
        sa.Column("code", sa.Text(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
    )
    op.create_table(
        "gdelt_cameo_codes",
        sa.Column("code", sa.Text(), primary_key=True),
    )
    op.bulk_insert(
        sa.table("gdelt_geo_countries", sa.column("code", sa.Text), sa.column("name", sa.Text)),
        [{"code": code, "name": name} for code, name in sorted(COUNTRY_NAME_BY_CODE.items())],
    )
    op.bulk_insert(
        sa.table("gdelt_cameo_codes", sa.column("code", sa.Text)),
        [{"code": code} for code in sorted(EVENT_LABELS)],
    )
    missing = ", ".join(f"'{value}'" for value in sorted(MISSING_COUNTRY_VALUES))
    sql = (Path(__file__).resolve().parents[2] / "sql/gdelt_code_correction.sql").read_text(encoding="utf-8")
    op.execute(sql.replace("__MISSING_COUNTRY_VALUES__", missing))


def downgrade() -> None:
    # Data repairs are compatible with v1.0. Restore a pre-upgrade backup to reverse them.
    op.drop_table("gdelt_code_correction_report")
    op.drop_table("gdelt_cameo_codes")
    op.drop_table("gdelt_geo_countries")
