"""Add x_oauth_sessions, x_connections, x_usage_counters -- the X
(Twitter) organic connector for Flyer Lady. Mirrors the Google Business
Profile migrations (0034/0035) for the OAuth session and connection
tables; x_usage_counters is new to this connector specifically, for the
per-tenant/global monthly post billing limits.
"""
from alembic import op
import sqlalchemy as sa

revision = "0037_x_connector"
down_revision = "0036_flyer_lady_publishing_started_at"
branch_labels = None
depends_on = None

_LOCATION_SCOPED_TABLES = ("x_oauth_sessions", "x_connections")


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = set(inspector.get_table_names())

    if "x_oauth_sessions" not in existing:
        op.create_table(
            "x_oauth_sessions",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("locations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("state_nonce", sa.String(128), nullable=False),
            sa.Column("code_verifier", sa.String(128), nullable=False),
            sa.Column("encrypted_refresh_token", sa.Text()),
            sa.Column("redirect_uri", sa.String(2000), nullable=False),
            sa.Column("status", sa.String(30), nullable=False, server_default="started"),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("consumed_at", sa.DateTime(timezone=True)),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint("state_nonce", name="uq_x_oauth_state_nonce"),
        )
    if not any(i["name"] == "ix_x_oauth_location_status" for i in inspector.get_indexes("x_oauth_sessions")):
        op.create_index("ix_x_oauth_location_status", "x_oauth_sessions", ["location_id", "status"])

    if "x_connections" not in existing:
        op.create_table(
            "x_connections",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("locations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("x_user_id", sa.String(100), nullable=False),
            sa.Column("x_username", sa.String(100)),
            sa.Column("encrypted_access_token", sa.Text(), nullable=False),
            sa.Column("encrypted_refresh_token", sa.Text()),
            sa.Column("token_expires_at", sa.DateTime(timezone=True)),
            sa.Column("token_key_version", sa.String(20), nullable=False, server_default="v1"),
            sa.Column("connection_status", sa.String(40), nullable=False, server_default="connected"),
            sa.Column("connected_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("last_health_check_at", sa.DateTime(timezone=True)),
            sa.UniqueConstraint("location_id", name="uq_x_connection_location"),
        )

    if "x_usage_counters" not in existing:
        op.create_table(
            "x_usage_counters",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("scope", sa.String(20), nullable=False),
            sa.Column("scope_key", sa.String(40), nullable=False),
            sa.Column("month_key", sa.String(7), nullable=False),
            sa.Column("count", sa.Integer(), nullable=False, server_default="0"),
            sa.UniqueConstraint("scope", "scope_key", "month_key", name="uq_x_usage_counter_scope"),
        )

    # x_usage_counters is deliberately NOT location-scoped RLS -- its
    # global-scope rows have no single location to isolate to, and its
    # tenant-scope rows are an aggregate count, not customer content;
    # flyer_lady/billing/x_spend_guard.py is the only code that touches
    # it, always with location_id passed explicitly rather than relying
    # on RLS to select it.
    if bind.dialect.name == "postgresql":
        for table in _LOCATION_SCOPED_TABLES:
            op.execute(sa.text(f'ALTER TABLE "{table}" ENABLE ROW LEVEL SECURITY'))
            op.execute(sa.text(f'ALTER TABLE "{table}" FORCE ROW LEVEL SECURITY'))
            policy = table + "_location_isolation"
            op.execute(sa.text(f'DROP POLICY IF EXISTS "{policy}" ON "{table}"'))
            sql = (
                f'CREATE POLICY "{policy}" ON "{table}" '
                "USING (location_id = NULLIF(current_setting('app.location_id', true), '')::integer) "
                "WITH CHECK (location_id = NULLIF(current_setting('app.location_id', true), '')::integer)"
            )
            op.execute(sa.text(sql))


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = set(inspector.get_table_names())
    if "x_usage_counters" in existing:
        op.drop_table("x_usage_counters")
    if "x_connections" in existing:
        op.drop_table("x_connections")
    if "x_oauth_sessions" in existing:
        try:
            op.drop_index("ix_x_oauth_location_status", table_name="x_oauth_sessions")
        except Exception:
            pass
        op.drop_table("x_oauth_sessions")
