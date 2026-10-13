"""Google Search ads from Flyer Lady: google_ads_connections.customer_id (the
workshop's chosen Google Ads account) and flyer_lady_boosts.platform so a
boost row can be a Facebook boost or a Google ad."""
from alembic import op
import sqlalchemy as sa

revision = "0046_google_ads_boosts"
down_revision = "0045_google_ads_connections"
branch_labels = None
depends_on = None


def _columns(table):
    return {c["name"] for c in sa.inspect(op.get_bind()).get_columns(table)}


def upgrade():
    if "customer_id" not in _columns("google_ads_connections"):
        op.add_column("google_ads_connections", sa.Column("customer_id", sa.String(20)))
    if "platform" not in _columns("flyer_lady_boosts"):
        op.add_column("flyer_lady_boosts", sa.Column("platform", sa.String(20), nullable=False, server_default="facebook"))


def downgrade():
    if "platform" in _columns("flyer_lady_boosts"):
        op.drop_column("flyer_lady_boosts", "platform")
    if "customer_id" in _columns("google_ads_connections"):
        op.drop_column("google_ads_connections", "customer_id")
