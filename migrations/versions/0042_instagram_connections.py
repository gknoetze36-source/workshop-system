"""Add instagram_connections -- Instagram API with Instagram Login, a separate
Meta app from Flyer Lady's Facebook Page connection. Mirrors
0038_threads_connector's threads_connections table and its RLS policy. No
OAuth-session table: Instagram Login authorizes exactly one account, so the
callback stores the connection directly."""
from alembic import op
import sqlalchemy as sa

revision = "0042_instagram_connections"
down_revision = "0041_location_daily_capacity"
branch_labels = None
depends_on = None

TABLE = "instagram_connections"


def upgrade():
    bind = op.get_bind()
    if TABLE not in set(sa.inspect(bind).get_table_names()):
        op.create_table(
            TABLE,
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("locations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("instagram_user_id", sa.String(100), nullable=False),
            sa.Column("instagram_username", sa.String(100)),
            sa.Column("encrypted_access_token", sa.Text(), nullable=False),
            sa.Column("token_expires_at", sa.DateTime(timezone=True)),
            sa.Column("token_refreshed_at", sa.DateTime(timezone=True)),
            sa.Column("connection_status", sa.String(40), nullable=False, server_default="connected"),
            sa.Column("connected_at", sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint("location_id", name="uq_instagram_connection_location"),
        )
    if bind.dialect.name == "postgresql":
        op.execute(sa.text(f'ALTER TABLE "{TABLE}" ENABLE ROW LEVEL SECURITY'))
        op.execute(sa.text(f'ALTER TABLE "{TABLE}" FORCE ROW LEVEL SECURITY'))
        policy = TABLE + "_location_isolation"
        op.execute(sa.text(f'DROP POLICY IF EXISTS "{policy}" ON "{TABLE}"'))
        op.execute(sa.text(
            f'CREATE POLICY "{policy}" ON "{TABLE}" '
            "USING (location_id = NULLIF(current_setting('app.location_id', true), '')::integer) "
            "WITH CHECK (location_id = NULLIF(current_setting('app.location_id', true), '')::integer)"
        ))


def downgrade():
    if TABLE in set(sa.inspect(op.get_bind()).get_table_names()):
        op.drop_table(TABLE)
