"""Guarantee locations.daily_capacity exists. It was only ever created inline by
database/owner_location.py's CREATE TABLE, never by a migration, and booking
availability now reads it (morning drop-offs are limited per day)."""
from alembic import op
import sqlalchemy as sa

revision = "0041_location_daily_capacity"
down_revision = "0040_location_billing_exempt"
branch_labels = None
depends_on = None


def upgrade():
    columns = {c["name"] for c in sa.inspect(op.get_bind()).get_columns("locations")}
    if "daily_capacity" not in columns:
        op.add_column("locations", sa.Column("daily_capacity", sa.Integer(), nullable=True, server_default="12"))


def downgrade():
    pass  # pre-existing column on most databases; never dropped
