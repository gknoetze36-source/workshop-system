"""Flyer Lady public social publishing foundation."""
from alembic import op
import sqlalchemy as sa

revision = "0011_flyer_lady_social_publishing"
down_revision = "0010_post_service_review_phase18"
branch_labels = None
depends_on = None

def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = set(inspector.get_table_names())

    # 0001 uses Base.metadata.create_all() on fresh databases. Because the
    # social models are now part of metadata, those two integration tables may
    # already exist when the full migration chain is run from zero. Existing
    # production databases will not have them, so create them only when absent.
    if "meta_social_oauth_sessions" not in existing:
        op.create_table("meta_social_oauth_sessions",
            sa.Column("id", sa.Integer(), primary_key=True), sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
            sa.Column("state_nonce", sa.String(128), nullable=False), sa.Column("encrypted_user_access_token", sa.Text(), nullable=False), sa.Column("redirect_uri", sa.String(2000), nullable=False),
            sa.Column("status", sa.String(30), nullable=False, server_default="started"), sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False), sa.Column("consumed_at", sa.DateTime(timezone=True)), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint("state_nonce", name="uq_meta_social_oauth_state_nonce"))
    if "meta_social_connections" not in existing:
        op.create_table("meta_social_connections",
            sa.Column("id", sa.Integer(), primary_key=True), sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
            sa.Column("page_id", sa.String(100), nullable=False), sa.Column("page_name", sa.String(255)), sa.Column("instagram_business_account_id", sa.String(100)), sa.Column("instagram_username", sa.String(255)),
            sa.Column("encrypted_page_access_token", sa.Text(), nullable=False), sa.Column("token_key_version", sa.String(20), nullable=False, server_default="v1"), sa.Column("token_expires_at", sa.DateTime(timezone=True)), sa.Column("permissions_json", sa.JSON()),
            sa.Column("connection_status", sa.String(40), nullable=False, server_default="connected"), sa.Column("connected_at", sa.DateTime(timezone=True), nullable=False), sa.Column("last_health_check_at", sa.DateTime(timezone=True)),
            sa.UniqueConstraint("tenant_id", name="uq_meta_social_connection_tenant"))
    if not any(i["name"] == "ix_meta_social_oauth_tenant_status" for i in inspector.get_indexes("meta_social_oauth_sessions")):
        op.create_index("ix_meta_social_oauth_tenant_status", "meta_social_oauth_sessions", ["tenant_id", "status"])

    if "flyer_lady_specials" not in existing:
        op.create_table("flyer_lady_specials",
            sa.Column("id", sa.Integer(), primary_key=True), sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False), sa.Column("created_by", sa.String(100), nullable=False), sa.Column("text", sa.Text(), nullable=False),
            sa.Column("media_url", sa.String(2000)), sa.Column("booking_link", sa.String(2000), nullable=False), sa.Column("status", sa.String(30), nullable=False, server_default="draft"), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False), sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False))
        op.create_index("ix_flyer_specials_tenant_status", "flyer_lady_specials", ["tenant_id", "status"])
        op.create_index("ix_flyer_specials_tenant_created", "flyer_lady_specials", ["tenant_id", "created_at"])
    if "flyer_lady_special_approvals" not in existing:
        op.create_table("flyer_lady_special_approvals",
            sa.Column("id", sa.Integer(), primary_key=True), sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False), sa.Column("special_id", sa.Integer(), sa.ForeignKey("flyer_lady_specials.id", ondelete="CASCADE"), nullable=False),
            sa.Column("decision", sa.String(20), nullable=False), sa.Column("decided_by", sa.String(100), nullable=False), sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False))
        op.create_index("ix_flyer_approvals_tenant_special", "flyer_lady_special_approvals", ["tenant_id", "special_id", "decided_at"])
    if "flyer_lady_special_posts" not in existing:
        op.create_table("flyer_lady_special_posts",
            sa.Column("id", sa.Integer(), primary_key=True), sa.Column("special_id", sa.Integer(), sa.ForeignKey("flyer_lady_specials.id", ondelete="CASCADE"), nullable=False), sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
            sa.Column("platform", sa.String(40), nullable=False), sa.Column("external_post_id", sa.String(255)), sa.Column("status", sa.String(30), nullable=False, server_default="pending"), sa.Column("published_at", sa.DateTime(timezone=True)), sa.Column("error_message", sa.Text()), sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"), sa.Column("next_attempt_at", sa.DateTime(timezone=True)), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False), sa.UniqueConstraint("special_id", "platform", name="uq_flyer_special_platform"))
        op.create_index("ix_flyer_posts_tenant_status", "flyer_lady_special_posts", ["tenant_id", "status"])
        op.create_index("ix_flyer_posts_special", "flyer_lady_special_posts", ["special_id"])
    if "flyer_lady_public_links" not in existing:
        op.create_table("flyer_lady_public_links",
            sa.Column("id", sa.Integer(), primary_key=True), sa.Column("special_id", sa.Integer(), sa.ForeignKey("flyer_lady_specials.id", ondelete="CASCADE"), nullable=False, unique=True), sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False), sa.Column("target_url", sa.String(2000), nullable=False), sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))
    if "flyer_lady_link_clicks" not in existing:
        op.create_table("flyer_lady_link_clicks",
            sa.Column("id", sa.Integer(), primary_key=True), sa.Column("special_id", sa.Integer(), sa.ForeignKey("flyer_lady_specials.id", ondelete="CASCADE"), nullable=False), sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False), sa.Column("user_agent", sa.String(1000)), sa.Column("referrer", sa.String(2000)), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))
        op.create_index("ix_flyer_clicks_special_created", "flyer_lady_link_clicks", ["special_id", "created_at"])
        op.create_index("ix_flyer_clicks_tenant_created", "flyer_lady_link_clicks", ["tenant_id", "created_at"])

    if bind.dialect.name == "postgresql":
        for table in ["meta_social_connections", "meta_social_oauth_sessions", "flyer_lady_specials", "flyer_lady_special_approvals", "flyer_lady_special_posts", "flyer_lady_link_clicks"]:
            op.execute(sa.text(f'ALTER TABLE "{table}" ENABLE ROW LEVEL SECURITY'))
            op.execute(sa.text(f'ALTER TABLE "{table}" FORCE ROW LEVEL SECURITY'))
            policy = table + "_tenant_isolation"
            op.execute(sa.text(f'DROP POLICY IF EXISTS "{policy}" ON "{table}"'))
            sql = (
                f'CREATE POLICY "{policy}" ON "{table}" '
                "USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::integer) "
                "WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::integer)"
            )
            op.execute(sa.text(sql))

def downgrade():
    bind = op.get_bind(); inspector = sa.inspect(bind); existing = set(inspector.get_table_names())
    for index, table in [("ix_flyer_clicks_tenant_created","flyer_lady_link_clicks"),("ix_flyer_clicks_special_created","flyer_lady_link_clicks"),("ix_flyer_posts_special","flyer_lady_special_posts"),("ix_flyer_posts_tenant_status","flyer_lady_special_posts"),("ix_flyer_approvals_tenant_special","flyer_lady_special_approvals"),("ix_flyer_specials_tenant_created","flyer_lady_specials"),("ix_flyer_specials_tenant_status","flyer_lady_specials")]:
        if table in existing:
            try: op.drop_index(index, table_name=table)
            except Exception: pass
    for table in ["flyer_lady_link_clicks","flyer_lady_public_links","flyer_lady_special_posts","flyer_lady_special_approvals","flyer_lady_specials"]:
        if table in existing: op.drop_table(table)
    # Social tables are also owned by the SQLAlchemy metadata and may have
    # been created by migration 0001 on a fresh database. Leave them intact.
