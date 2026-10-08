"""Add locations.billing_exempt: demo/internal workshops (e.g. the one Meta's
App Review uses) are never billed and never shown the payment wall."""
from alembic import op
import sqlalchemy as sa

revision = "0040_location_billing_exempt"
down_revision = "0039_tiktok_connector"
branch_labels = None
depends_on = None


def upgrade():
    columns = {c["name"] for c in sa.inspect(op.get_bind()).get_columns("locations")}
    if "billing_exempt" not in columns:
        op.add_column("locations", sa.Column("billing_exempt", sa.Boolean(), nullable=False, server_default=sa.false()))


def downgrade():
    columns = {c["name"] for c in sa.inspect(op.get_bind()).get_columns("locations")}
    if "billing_exempt" in columns:
        op.drop_column("locations", "billing_exempt")
