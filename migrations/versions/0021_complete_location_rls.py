"""Complete RLS coverage for location-scoped tables missing it.

Found during a full architecture-compliance audit (2026-08-24): a sweep of
every table with a location_id column showed 20 had no RLS enabled at all.
Confirmed empirically under a properly restricted, non-superuser role
(matching create_phanta_app_role.py's intent) that this is not just a
theoretical gap -- app-level `WHERE location_id = %s` filtering in the
repositories was the ONLY thing protecting these tables; nothing at the
database layer would stop a bug (or a future engineer) from reading or
writing across locations.

This migration covers the 16 that are unambiguously per-location
operational data with real cross-tenant leak risk if left unprotected:
automation_rules, automation_logs, scheduled_jobs, failed_jobs (contains
error messages, which can embed customer/booking data), billing_records,
service_rules, services, chatbot_messages, chatbot_usage_daily,
chatbot_usage_monthly, communication_logs, reminder_campaigns,
service_prices, usage_daily, credential_audit, feature_flags.

Deliberately NOT covered here:
  - users: must resolve before location context exists (unchanged reasoning
    from every other users-related decision in this codebase)
  - onboarding_answers, onboarding_sessions, onboarding_state: location_id
    is nullable on these because onboarding steps can happen before a
    location row fully exists. RLS on these needs the same NULL-tolerant
    pattern meta_webhook_events uses (location_id IS NULL OR location_id =
    ...), which needs verification against the actual onboarding flow to
    get right -- left as a follow-up rather than rushed here.
"""
from alembic import op
import sqlalchemy as sa

revision = "0021_complete_location_rls"
down_revision = "0020_automation_location_ownership"
branch_labels = None
depends_on = None

TABLES = [
    "automation_rules", "automation_logs", "scheduled_jobs", "failed_jobs",
    "billing_records", "service_rules", "services", "chatbot_messages",
    "chatbot_usage_daily", "chatbot_usage_monthly", "communication_logs",
    "reminder_campaigns", "service_prices", "usage_daily",
    "credential_audit", "feature_flags",
]


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    for table in TABLES:
        if table not in existing_tables:
            continue
        columns = {c["name"] for c in inspector.get_columns(table)}
        if "location_id" not in columns:
            continue

        op.execute(sa.text(f'ALTER TABLE "{table}" ENABLE ROW LEVEL SECURITY'))
        op.execute(sa.text(f'ALTER TABLE "{table}" FORCE ROW LEVEL SECURITY'))

        isolation_policy = f"{table}_location_isolation"
        op.execute(sa.text(f'DROP POLICY IF EXISTS "{isolation_policy}" ON "{table}"'))
        op.execute(sa.text(
            f'CREATE POLICY "{isolation_policy}" ON "{table}" '
            "USING (location_id = NULLIF(current_setting('app.location_id', true), '')::integer) "
            "WITH CHECK (location_id = NULLIF(current_setting('app.location_id', true), '')::integer)"
        ))

        admin_policy = f"{table}_platform_admin_read"
        op.execute(sa.text(f'DROP POLICY IF EXISTS "{admin_policy}" ON "{table}"'))
        op.execute(sa.text(
            f'CREATE POLICY "{admin_policy}" ON "{table}" '
            "FOR SELECT TO public "
            "USING (current_setting('app.platform_admin', true) = '1')"
        ))


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())
    for table in TABLES:
        if table not in existing_tables:
            continue
        op.execute(sa.text(f'DROP POLICY IF EXISTS "{table}_platform_admin_read" ON "{table}"'))
        op.execute(sa.text(f'DROP POLICY IF EXISTS "{table}_location_isolation" ON "{table}"'))
        op.execute(sa.text(f'ALTER TABLE "{table}" DISABLE ROW LEVEL SECURITY'))
