"""Add tiktok_oauth_sessions, tiktok_connections -- the TikTok organic
PHOTO Direct Post connector for Flyer Lady (private/beta). Mirrors
0038_threads_connector's shape, plus creator_nickname/
allowed_privacy_levels/selected_privacy_level on the connection, which
the required flow (creator_info -> show nickname -> user selects an
allowed privacy level) and the "do not silently choose a privacy
level" requirement both need somewhere durable to live.
"""
from alembic import op
import sqlalchemy as sa

revision = "0039_tiktok_connector"
down_revision = "0038_threads_connector"
branch_labels = None
depends_on = None

_LOCATION_SCOPED_TABLES = ("tiktok_oauth_sessions", "tiktok_connections")


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = set(inspector.get_table_names())

    if "tiktok_oauth_sessions" not in existing:
        op.create_table(
            "tiktok_oauth_sessions",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("locations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("state_nonce", sa.String(128), nullable=False),
            sa.Column("encrypted_access_token", sa.Text()),
            sa.Column("encrypted_refresh_token", sa.Text()),
            sa.Column("redirect_uri", sa.String(2000), nullable=False),
            sa.Column("status", sa.String(30), nullable=False, server_default="started"),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("consumed_at", sa.DateTime(timezone=True)),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint("state_nonce", name="uq_tiktok_oauth_state_nonce"),
        )
    if not any(i["name"] == "ix_tiktok_oauth_location_status" for i in inspector.get_indexes("tiktok_oauth_sessions")):
        op.create_index("ix_tiktok_oauth_location_status", "tiktok_oauth_sessions", ["location_id", "status"])

    if "tiktok_connections" not in existing:
        op.create_table(
            "tiktok_connections",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("locations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("tiktok_open_id", sa.String(100), nullable=False),
            sa.Column("tiktok_username", sa.String(100)),
            sa.Column("creator_nickname", sa.String(255)),
            sa.Column("allowed_privacy_levels", sa.JSON()),
            sa.Column("selected_privacy_level", sa.String(40)),
            sa.Column("encrypted_access_token", sa.Text(), nullable=False),
            sa.Column("encrypted_refresh_token", sa.Text(), nullable=False),
            sa.Column("token_expires_at", sa.DateTime(timezone=True)),
            sa.Column("token_key_version", sa.String(20), nullable=False, server_default="v1"),
            sa.Column("connection_status", sa.String(40), nullable=False, server_default="connected"),
            sa.Column("connected_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("last_health_check_at", sa.DateTime(timezone=True)),
            sa.UniqueConstraint("location_id", name="uq_tiktok_connection_location"),
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
    if "tiktok_connections" in existing:
        op.drop_table("tiktok_connections")
    if "tiktok_oauth_sessions" in existing:
        try:
            op.drop_index("ix_tiktok_oauth_location_status", table_name="tiktok_oauth_sessions")
        except Exception:
            pass
        op.drop_table("tiktok_oauth_sessions")
