"""PHANTA Phase 2 database foundation."""
from alembic import op
import sqlalchemy as sa

revision = "0001_phase2_foundation"
down_revision = None
branch_labels = None
depends_on = None

TENANT_TABLES = [
    "customers", "vehicles", "bookings", "service_records", "conversations",
    "messages", "recommendations", "quotes", "quote_line_items", "approvals",
    "follow_ups", "tasks", "audit_logs", "conversation_summaries",
    "tool_executions", "meta_business_connections",
    "meta_business_verification_status", "meta_permissions_grants",
    "meta_webhook_events", "meta_audit_logs", "payment_customers", "payments",
    "subscriptions", "invoices", "refunds", "paystack_webhook_events",
    "ai_usage_log", "meta_social_connections", "meta_social_oauth_sessions", "flyer_lady_specials", "flyer_lady_special_approvals", "flyer_lady_special_posts", "flyer_lady_link_clicks",
]

def upgrade():
    from models.core import Base
    from models import integration_models  # noqa: F401
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)
    if bind.dialect.name != "postgresql":
        return

    # Fail fast if the migration's RLS contract drifts from the model schema.
    for table in TENANT_TABLES:
        if "tenant_id" not in Base.metadata.tables[table].c:
            raise RuntimeError(f"RLS table {table} has no tenant_id column")

    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    op.execute("""
        ALTER TABLE bookings
        ADD CONSTRAINT bookings_no_bay_overlap
        EXCLUDE USING gist (
            tenant_id WITH =,
            tstzrange(start_time, end_time, '[)') WITH &&,
            bay_id WITH =
        )
        WHERE (bay_id IS NOT NULL AND status NOT IN ('cancelled', 'completed'))
    """)
    op.execute("""
        ALTER TABLE bookings
        ADD CONSTRAINT bookings_no_technician_overlap
        EXCLUDE USING gist (
            tenant_id WITH =,
            tstzrange(start_time, end_time, '[)') WITH &&,
            technician_id WITH =
        )
        WHERE (technician_id IS NOT NULL AND status NOT IN ('cancelled', 'completed'))
    """)
    for table in TENANT_TABLES:
        op.execute(sa.text(f'ALTER TABLE "{table}" ENABLE ROW LEVEL SECURITY'))
        op.execute(sa.text(f'ALTER TABLE "{table}" FORCE ROW LEVEL SECURITY'))
        policy = table + "_tenant_isolation"
        sql = (
            'CREATE POLICY "' + policy + '" ON "' + table + '" '
            "USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::integer) "
            "WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::integer)"
        )
        op.execute(sa.text(sql))
    # Approvals are records of customer consent and must be immutable.
    op.execute("""
        CREATE OR REPLACE FUNCTION phanta_reject_approval_mutation()
        RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'approval records are append-only';
        END;
        $$ LANGUAGE plpgsql
    """)
    op.execute("""
        CREATE TRIGGER approvals_append_only
        BEFORE UPDATE OR DELETE ON approvals
        FOR EACH ROW EXECUTE FUNCTION phanta_reject_approval_mutation()
    """)

def downgrade():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        for table in reversed(TENANT_TABLES):
            op.execute(sa.text(f'DROP POLICY IF EXISTS "{table}_tenant_isolation" ON "{table}"'))
            op.execute(sa.text(f'ALTER TABLE "{table}" DISABLE ROW LEVEL SECURITY'))
        op.execute("ALTER TABLE bookings DROP CONSTRAINT IF EXISTS bookings_no_bay_overlap")
        op.execute("ALTER TABLE bookings DROP CONSTRAINT IF EXISTS bookings_no_technician_overlap")
        op.execute("DROP TRIGGER IF EXISTS approvals_append_only ON approvals")
        op.execute("DROP FUNCTION IF EXISTS phanta_reject_approval_mutation()")
    from models.core import Base
    from models import integration_models  # noqa: F401
    Base.metadata.drop_all(bind=bind)
