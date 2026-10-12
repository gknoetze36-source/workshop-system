"""bookings.flyer_special_id: which Flyer Lady special a web booking came from,
so the Results page can show a workshop the bookings each post brought in."""
from alembic import op
import sqlalchemy as sa

revision = "0044_booking_flyer_special"
down_revision = "0043_meta_ads_boosts"
branch_labels = None
depends_on = None


def upgrade():
    columns = {c["name"] for c in sa.inspect(op.get_bind()).get_columns("bookings")}
    if "flyer_special_id" not in columns:
        op.add_column("bookings", sa.Column("flyer_special_id", sa.Integer(), nullable=True))


def downgrade():
    columns = {c["name"] for c in sa.inspect(op.get_bind()).get_columns("bookings")}
    if "flyer_special_id" in columns:
        op.drop_column("bookings", "flyer_special_id")
