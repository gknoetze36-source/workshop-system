"""Allow PHANTA platform admins to read tenant-scoped audit data.

Tenant RLS remains the default. Platform access is an explicit, transaction-
local SELECT-only context set by database.get_platform_session(). No write
policy is added for platform admins.
"""
from alembic import op
import sqlalchemy as sa

revision = "0012_platform_admin_read_policy"
down_revision = "0011_flyer_lady_social_publishing"
branch_labels = None
depends_on = None

PLATFORM_TABLES = [
    "customers", "vehicles", "bookings", "service_records", "conversations",
    "messages", "recommendations", "quotes", "quote_line_items", "approvals",
    "follow_ups", "tasks", "audit_logs", "conversation_summaries",
    "tool_executions", "meta_business_connections",
    "meta_business_verification_status", "meta_permissions_grants",
    "meta_signup_sessions", "meta_webhook_events", "meta_audit_logs",
    "meta_message_templates", "meta_message_attempts", "payment_customers",
    "payments", "subscriptions", "invoices", "refunds",
    "paystack_webhook_events", "ai_usage_log", "booking_confirmations",
    "meta_social_connections", "meta_social_oauth_sessions",
    "flyer_lady_specials", "flyer_lady_special_approvals",
    "flyer_lady_special_posts", "flyer_lady_public_links", "flyer_lady_link_clicks",
]

def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    inspector = sa.inspect(bind)
    existing = set(inspector.get_table_names())
    for table in PLATFORM_TABLES:
        if table not in existing:
            continue
        columns = {c["name"] for c in inspector.get_columns(table)}
        if "location_id" not in columns:
            continue
        # 0011 created flyer_lady_public_links with location_id but did not
        # enable RLS on it. Bring it under the same location isolation contract
        # before adding the platform-admin read policy.
        if table == "flyer_lady_public_links":
            op.execute(sa.text('ALTER TABLE "flyer_lady_public_links" ENABLE ROW LEVEL SECURITY'))
            op.execute(sa.text('ALTER TABLE "flyer_lady_public_links" FORCE ROW LEVEL SECURITY'))
            op.execute(sa.text('DROP POLICY IF EXISTS "flyer_lady_public_links_location_isolation" ON "flyer_lady_public_links"'))
            op.execute(sa.text(
                'CREATE POLICY "flyer_lady_public_links_location_isolation" ON "flyer_lady_public_links" '
                "USING (location_id = NULLIF(current_setting('app.location_id', true), '')::integer) "
                "WITH CHECK (location_id = NULLIF(current_setting('app.location_id', true), '')::integer)"
            ))
        policy = f"{table}_platform_admin_read"
        op.execute(sa.text(f'DROP POLICY IF EXISTS "{policy}" ON "{table}"'))
        op.execute(sa.text(
            f'CREATE POLICY "{policy}" ON "{table}" FOR SELECT '
            "USING (current_setting('app.platform_admin', true) = '1')"
        ))

def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    inspector = sa.inspect(bind)
    existing = set(inspector.get_table_names())
    for table in PLATFORM_TABLES:
        if table in existing:
            op.execute(sa.text(f'DROP POLICY IF EXISTS "{table}_platform_admin_read" ON "{table}"'))
