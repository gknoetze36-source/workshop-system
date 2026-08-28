"""Repair audit_logs.location_id integrity.

THE MISMATCH
------------
models/core.py declares AuditLog.location_id as:

    ForeignKey("locations.id", ondelete="CASCADE"), nullable=False

but the physical column was never created that way. database/owner_location.py
adds it with a bare _add_column() as a nullable INTEGER with no foreign key,
and Base.metadata.create_all() does not retrofit constraints onto a table that
already exists. So neither the NOT NULL nor the cascade contract the ORM
believes in was ever enforced by the database.

WHAT THIS MIGRATION DOES
------------------------
1. Backfills location_id where it can be derived from the acting user.
2. Adds the missing foreign key with ON DELETE CASCADE, so deleting a location
   removes its audit rows rather than orphaning them -- which matters for
   tenant offboarding.

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
It does not add NOT NULL. Platform-administrator actions legitimately occur
outside any location, so a blanket NOT NULL would break them. Tenant-less
authentication events now have their own home in security_events
(0025_security_events) rather than being forced into this table.

If a future review establishes that every remaining row must belong to a
location, NOT NULL can be added then -- but only after the count reported
below reaches zero.
"""
from alembic import op
import sqlalchemy as sa

revision = "0026_audit_logs_location_fk"
down_revision = "0025_security_events"
branch_labels = None
depends_on = None

FK_NAME = "fk_audit_logs_location_id"


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    inspector = sa.inspect(bind)
    if "audit_logs" not in set(inspector.get_table_names()):
        return

    # CRITICAL: migrations run as the application role, which is subject to
    # FORCE ROW LEVEL SECURITY on audit_logs. With no app.location_id set, that
    # role sees NO rows -- so every data statement below silently affects zero
    # rows, and the foreign key gets "validated" against an empty result set
    # while violating rows remain in the table. Verified on a real PostgreSQL
    # upgrade: an audit row pointing at a deleted location survived, and
    # pg_constraint reported convalidated = true.
    #
    # Setting app.platform_admin is NOT sufficient -- the platform policy on
    # audit_logs grants SELECT only, so UPDATE stays blocked. The migration
    # runs as the table owner, so it disables RLS for the repair and restores
    # it immediately afterwards.
    op.execute(sa.text('ALTER TABLE "audit_logs" DISABLE ROW LEVEL SECURITY'))

    # 1. Backfill from the acting user where possible. Only fills rows that are
    #    currently NULL, and only where the user still resolves to a location.
    op.execute(sa.text("""
        UPDATE audit_logs AS al
        SET location_id = u.location_id
        FROM users AS u
        WHERE al.location_id IS NULL
          AND al.actor_user_id = u.id
          AND u.location_id IS NOT NULL
    """))

    # 2. Drop any orphaned reference before adding the constraint, otherwise
    #    the ALTER fails on historic rows pointing at deleted locations.
    op.execute(sa.text("""
        UPDATE audit_logs
        SET location_id = NULL
        WHERE location_id IS NOT NULL
          AND location_id NOT IN (SELECT id FROM locations)
    """))

    # Re-check for rows the constraint would reject, now that they are visible.
    remaining = bind.execute(sa.text("""
        SELECT COUNT(*) FROM audit_logs
        WHERE location_id IS NOT NULL
          AND location_id NOT IN (SELECT id FROM locations)
    """)).scalar()
    if remaining:
        raise RuntimeError(
            f"{remaining} audit_logs row(s) still reference a missing location; "
            "refusing to add a foreign key that would be validated against them"
        )

    existing = {fk.get("name") for fk in inspector.get_foreign_keys("audit_logs")}
    if FK_NAME not in existing:
        op.create_foreign_key(
            FK_NAME,
            "audit_logs",
            "locations",
            ["location_id"],
            ["id"],
            ondelete="CASCADE",
        )

    # Restore protection. This must happen even though the constraint work is
    # done -- leaving RLS off would silently expose every tenant's audit trail
    # to every other tenant.
    op.execute(sa.text('ALTER TABLE "audit_logs" ENABLE ROW LEVEL SECURITY'))
    op.execute(sa.text('ALTER TABLE "audit_logs" FORCE ROW LEVEL SECURITY'))


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    bind.execute(sa.text("SELECT set_config('app.platform_admin', '1', false)"))
    op.execute(sa.text(f'ALTER TABLE "audit_logs" DROP CONSTRAINT IF EXISTS "{FK_NAME}"'))
