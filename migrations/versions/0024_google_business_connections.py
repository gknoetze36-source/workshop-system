"""Add google_business_connections for Google Business Profile posting.

Not Calendar (see models/integration_models.py's module docstring for
why Calendar stays out of scope) -- this is Local Posts, a genuinely
free-at-the-API-level product distinct from Calendar, and the highest-
reach channel for local businesses per the research behind this feature
(Google Business Profile posts see more impressions than Instagram or
Facebook for local businesses specifically).

Mirrors migration 0011's meta_social_connections table structure and RLS
setup exactly -- same one-connection-per-location shape, same
location_id-scoped RLS policy.
"""
from alembic import op
import sqlalchemy as sa

revision = "0024_google_business_connections"
down_revision = "0023_location_access_lock"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = set(inspector.get_table_names())

    if "google_business_connections" not in existing:
        op.create_table(
            "google_business_connections",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("locations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("google_account_id", sa.String(100), nullable=False),
            sa.Column("google_location_id", sa.String(100), nullable=False),
            sa.Column("business_name", sa.String(255)),
            sa.Column("encrypted_refresh_token", sa.Text(), nullable=False),
            sa.Column("token_key_version", sa.String(20), nullable=False, server_default="v1"),
            sa.Column("connection_status", sa.String(40), nullable=False, server_default="connected"),
            sa.Column("connected_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("last_health_check_at", sa.DateTime(timezone=True)),
            sa.UniqueConstraint("location_id", name="uq_google_business_connection_location"),
        )

    if bind.dialect.name == "postgresql":
        op.execute(sa.text('ALTER TABLE "google_business_connections" ENABLE ROW LEVEL SECURITY'))
        op.execute(sa.text('ALTER TABLE "google_business_connections" FORCE ROW LEVEL SECURITY'))
        op.execute(sa.text('DROP POLICY IF EXISTS "google_business_connections_location_isolation" ON "google_business_connections"'))
        op.execute(sa.text(
            'CREATE POLICY "google_business_connections_location_isolation" ON "google_business_connections" '
            "USING (location_id = NULLIF(current_setting('app.location_id', true), '')::integer) "
            "WITH CHECK (location_id = NULLIF(current_setting('app.location_id', true), '')::integer)"
        ))


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = set(inspector.get_table_names())
    if bind.dialect.name == "postgresql":
        op.execute(sa.text('DROP POLICY IF EXISTS "google_business_connections_location_isolation" ON "google_business_connections"'))
    if "google_business_connections" in existing:
        op.drop_table("google_business_connections")
