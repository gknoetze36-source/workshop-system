"""Add threads_oauth_sessions, threads_connections -- the Threads
organic connector for Flyer Lady. Mirrors 0037_x_connector's shape,
minus a PKCE column (Threads' OAuth flow doesn't use it) and minus a
usage-counter table (no billing requirement for Threads, unlike X).
"""
from alembic import op
import sqlalchemy as sa

revision = "0038_threads_connector"
down_revision = "0037_x_connector"
branch_labels = None
depends_on = None

_LOCATION_SCOPED_TABLES = ("threads_oauth_sessions", "threads_connections")


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = set(inspector.get_table_names())

    if "threads_oauth_sessions" not in existing:
        op.create_table(
            "threads_oauth_sessions",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("locations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("state_nonce", sa.String(128), nullable=False),
            sa.Column("encrypted_long_lived_token", sa.Text()),
            sa.Column("redirect_uri", sa.String(2000), nullable=False),
            sa.Column("status", sa.String(30), nullable=False, server_default="started"),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("consumed_at", sa.DateTime(timezone=True)),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint("state_nonce", name="uq_threads_oauth_state_nonce"),
        )
    if not any(i["name"] == "ix_threads_oauth_location_status" for i in inspector.get_indexes("threads_oauth_sessions")):
        op.create_index("ix_threads_oauth_location_status", "threads_oauth_sessions", ["location_id", "status"])

    if "threads_connections" not in existing:
        op.create_table(
            "threads_connections",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("locations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("threads_user_id", sa.String(100), nullable=False),
            sa.Column("threads_username", sa.String(100)),
            sa.Column("encrypted_long_lived_token", sa.Text(), nullable=False),
            sa.Column("token_expires_at", sa.DateTime(timezone=True)),
            sa.Column("token_key_version", sa.String(20), nullable=False, server_default="v1"),
            sa.Column("connection_status", sa.String(40), nullable=False, server_default="connected"),
            sa.Column("connected_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("last_health_check_at", sa.DateTime(timezone=True)),
            sa.UniqueConstraint("location_id", name="uq_threads_connection_location"),
        )

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
    if "threads_connections" in existing:
        op.drop_table("threads_connections")
    if "threads_oauth_sessions" in existing:
        try:
            op.drop_index("ix_threads_oauth_location_status", table_name="threads_oauth_sessions")
        except Exception:
            pass
        op.drop_table("threads_oauth_sessions")
