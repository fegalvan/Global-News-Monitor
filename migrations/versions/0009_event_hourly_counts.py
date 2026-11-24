"""Install transactional hourly live and historical counts."""

from pathlib import Path

from alembic import op

revision = "0009_event_hourly_counts"
down_revision = "0008_gdelt_codes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Block concurrent writers while seeding and installing the triggers.
    op.execute("LOCK TABLE normalized_events, gdelt_export_checkpoints IN SHARE ROW EXCLUSIVE MODE")
    op.execute((Path(__file__).resolve().parents[2] / "sql/event_hourly_counts.sql").read_text(encoding="utf-8"))


def downgrade() -> None:
    op.execute("""
        DROP TRIGGER count_normalized_inserts ON normalized_events;
        DROP TRIGGER count_normalized_deletes ON normalized_events;
        DROP TRIGGER count_normalized_update_removals ON normalized_events;
        DROP TRIGGER count_normalized_update_writes ON normalized_events;
        DROP TRIGGER record_completed_export ON gdelt_export_checkpoints;
        DROP FUNCTION count_inserted_events(), count_removed_events(),
            record_completed_export(), prune_event_history();
        DROP TABLE event_history_coverage, event_hourly_counts;
    """)
