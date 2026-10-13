"""Add google_ads_connections for the Google Ads OAuth connect flow.

The old google_business_* tables are intentionally left in place (the
Google Business integration was removed from code only). RLS policy as 0043."""
from alembic import op
import sqlalchemy as sa

revision = "0045_google_ads_connections"
down_revision = "0044_booking_flyer_special"
branch_labels = None
depends_on = None

TABLE = "google_ads_connections"


def upgrade():
    bind = op.get_bind()
    if TABLE not in set(sa.inspect(bind).get_table_names()):
        op.create_table(
            TABLE,
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("locations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("encrypted_refresh_token", sa.Text(), nullable=False),
            sa.Column("token_key_version", sa.String(20), nullable=False, server_default="v1"),
            sa.Column("connection_status", sa.String(40), nullable=False, server_default="connected"),
            sa.Column("connected_at", sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint("location_id", name="uq_google_ads_connection_location"),
        )
    if bind.dialect.name == "postgresql":
        policy = TABLE + "_location_isolation"
        op.execute(sa.text(f'ALTER TABLE "{TABLE}" ENABLE ROW LEVEL SECURITY'))
        op.execute(sa.text(f'ALTER TABLE "{TABLE}" FORCE ROW LEVEL SECURITY'))
        op.execute(sa.text(f'DROP POLICY IF EXISTS "{policy}" ON "{TABLE}"'))
        op.execute(sa.text(
            f'CREATE POLICY "{policy}" ON "{TABLE}" '
            "USING (location_id = NULLIF(current_setting('app.location_id', true), '')::integer) "
            "WITH CHECK (location_id = NULLIF(current_setting('app.location_id', true), '')::integer)"
        ))


def downgrade():
    if TABLE in set(sa.inspect(op.get_bind()).get_table_names()):
        op.drop_table(TABLE)
