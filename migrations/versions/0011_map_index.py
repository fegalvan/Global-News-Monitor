"""Cover the viewport query with a partial index on geolocated events."""

from alembic import op

revision = "0011_map_index"
down_revision = "0010_scheduler_health"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_map_coordinates
        ON normalized_events (event_time_utc) INCLUDE (latitude, longitude)
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """)


def downgrade() -> None:
    op.drop_index("idx_events_map_coordinates", table_name="normalized_events")
