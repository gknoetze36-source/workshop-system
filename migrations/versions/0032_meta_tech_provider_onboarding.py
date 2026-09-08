"""Meta Tech Provider onboarding state on meta_business_connections.

Successful Embedded Signup previously set connection_status='connected'
immediately. The Tech Provider onboarding run (WABA verification, System User
assignment, phone registration, credit line sharing, WABA webhook
subscription, template sync, final verification) now decides that, so the
connection needs somewhere to record how far it got.

Backward compatibility
----------------------
Rows that are already 'connected' predate the orchestrator. They are stamped
with onboarding_step='legacy_connected' so they keep working exactly as they
do today: they are never auto-invalidated, never forced through onboarding,
and never reported as incomplete. Only connections created from here on run
the full orchestrator.
"""
from alembic import op
import sqlalchemy as sa

revision = "0032_meta_tech_provider_onboarding"
down_revision = "0031_meta_social_user_identity"
branch_labels = None
depends_on = None

TABLE = "meta_business_connections"

# (column name, type) -- all nullable so the migration is safe on a populated
# production table and needs no server_default rewrite of existing rows.
NEW_COLUMNS = (
    ("onboarding_step", sa.String(length=40)),
    ("last_successful_onboarding_step", sa.String(length=40)),
    ("last_onboarding_attempt_at", sa.DateTime(timezone=True)),
    ("last_onboarding_error", sa.Text()),
    ("owner_business_id", sa.String(length=100)),
    ("waba_currency", sa.String(length=10)),
    ("system_user_id", sa.String(length=100)),
    ("system_user_assigned_at", sa.DateTime(timezone=True)),
    ("phone_registered_at", sa.DateTime(timezone=True)),
    ("credit_line_id", sa.String(length=100)),
    ("credit_allocation_config_id", sa.String(length=100)),
    ("credit_shared_at", sa.DateTime(timezone=True)),
    ("credit_verified_at", sa.DateTime(timezone=True)),
    ("webhook_subscribed_at", sa.DateTime(timezone=True)),
    ("webhook_verified_at", sa.DateTime(timezone=True)),
    ("templates_synced_at", sa.DateTime(timezone=True)),
)


def _existing_columns(bind):
    inspector = sa.inspect(bind)
    if TABLE not in set(inspector.get_table_names()):
        return None
    return {c["name"] for c in inspector.get_columns(TABLE)}


def upgrade():
    bind = op.get_bind()
    columns = _existing_columns(bind)
    if columns is None:
        return

    for name, column_type in NEW_COLUMNS:
        if name not in columns:
            op.add_column(TABLE, sa.Column(name, column_type, nullable=True))

    # Grandfather every connection that is already live. Without this an
    # existing workshop would read as "onboarding incomplete" the moment this
    # deploys, and messaging_provider would stop sending for them.
    op.execute(
        sa.text(
            f"UPDATE {TABLE} "
            "SET onboarding_step = 'legacy_connected', "
            "    last_successful_onboarding_step = 'legacy_connected' "
            "WHERE onboarding_step IS NULL "
            "  AND connection_status IN ('connected', 'expiring_soon')"
        )
    )

    # Connections that exist but were never live get an honest starting point
    # rather than a NULL that later code has to guess about.
    op.execute(
        sa.text(
            f"UPDATE {TABLE} "
            "SET onboarding_step = 'signup_received' "
            "WHERE onboarding_step IS NULL"
        )
    )

    if bind.dialect.name == "postgresql":
        op.execute(
            sa.text(
                f"CREATE INDEX IF NOT EXISTS ix_{TABLE}_onboarding_step "
                f"ON {TABLE} (onboarding_step)"
            )
        )


def downgrade():
    bind = op.get_bind()
    columns = _existing_columns(bind)
    if columns is None:
        return

    if bind.dialect.name == "postgresql":
        op.execute(sa.text(f"DROP INDEX IF EXISTS ix_{TABLE}_onboarding_step"))

    for name, _ in reversed(NEW_COLUMNS):
        if name in columns:
            op.drop_column(TABLE, name)
