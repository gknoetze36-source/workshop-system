"""Add options_json to google_business_oauth_sessions -- the exact
account/location list Google returned during the callback, so
/connect/complete can validate the submitted selection against it
instead of trusting account_id/google_location_id from the browser
outright.
"""
from alembic import op
import sqlalchemy as sa

revision = "0035_google_business_oauth_options"
down_revision = "0034_google_business_oauth_sessions"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {c["name"] for c in inspector.get_columns("google_business_oauth_sessions")}
    if "options_json" not in columns:
        op.add_column("google_business_oauth_sessions", sa.Column("options_json", sa.JSON(), nullable=True))


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {c["name"] for c in inspector.get_columns("google_business_oauth_sessions")}
    if "options_json" in columns:
        op.drop_column("google_business_oauth_sessions", "options_json")
