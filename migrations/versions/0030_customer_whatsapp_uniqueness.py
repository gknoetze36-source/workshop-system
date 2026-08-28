"""One customer record per WhatsApp number per workshop.

WHY
---
`customers` had only a non-unique index on (location_id, phone, email). Nothing
prevented two rows for the same WhatsApp number within the same workshop.

The risk is concurrent inbound webhooks: two messages from the same number
arriving close together can each find no existing customer and each create one.
The workshop then sees the same person twice, with their history split across
both records.

PHANTA's webhook idempotency (meta_webhook_events.external_event_id UNIQUE,
checked before processing) makes this much less likely than it would otherwise
be -- a duplicated delivery of the SAME message is rejected outright. But two
genuinely different messages arriving simultaneously are not duplicates, and
nothing stopped them racing.

SCOPE OF THE CONSTRAINT
-----------------------
Partial unique index on (location_id, whatsapp_number), excluding:

  * NULL whatsapp_number -- a customer captured by phone or email only is
    legitimate and several may exist;
  * soft-deleted rows (deleted_at IS NOT NULL) -- erasure rewrites the number
    to "deleted:<id>@invalid", which is already unique per row, and a deleted
    record must never block a new customer with the same number from being
    created later.

It is scoped per location, not globally: the same person may legitimately be a
customer of two different workshops, and each workshop owns its own record.

EXISTING DUPLICATES
-------------------
If duplicates already exist this migration RAISES rather than merging them.
Merging customer records is a business decision -- which name wins, which
vehicles and bookings move -- and silently guessing would risk losing data or
attributing one person's service history to another. The error names the
offending rows so they can be resolved deliberately, then the migration re-run.

ROW LEVEL SECURITY
------------------
`customers` has FORCE ROW LEVEL SECURITY and migrations run with no
app.location_id set, so the duplicate-detection query would see zero rows and
report a false all-clear. RLS is therefore disabled for the check and restored
immediately afterwards. See 0026 for the full explanation of this failure mode.
"""
from alembic import op
import sqlalchemy as sa

revision = "0030_customer_whatsapp_uniqueness"
down_revision = "0029_owner_business_identity"
branch_labels = None
depends_on = None

INDEX_NAME = "uq_customer_location_whatsapp_number"


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    inspector = sa.inspect(bind)
    if "customers" not in set(inspector.get_table_names()):
        return
    columns = {c["name"] for c in inspector.get_columns("customers")}
    if "whatsapp_number" not in columns:
        return
    if INDEX_NAME in {i["name"] for i in inspector.get_indexes("customers")}:
        return

    # See the module docstring: without this the check below sees no rows.
    op.execute(sa.text('ALTER TABLE "customers" DISABLE ROW LEVEL SECURITY'))
    try:
        duplicates = bind.execute(sa.text("""
            SELECT location_id, whatsapp_number, COUNT(*) AS n
            FROM customers
            WHERE whatsapp_number IS NOT NULL
              AND deleted_at IS NULL
            GROUP BY location_id, whatsapp_number
            HAVING COUNT(*) > 1
            ORDER BY n DESC
            LIMIT 20
        """)).fetchall()

        if duplicates:
            detail = "; ".join(
                f"location {row[0]} number {row[1]} x{row[2]}" for row in duplicates
            )
            raise RuntimeError(
                "cannot add per-workshop customer uniqueness: duplicate WhatsApp "
                f"numbers already exist ({detail}). Merge or remove these records "
                "first -- deciding which record survives is a business decision "
                "this migration will not make for you."
            )

        op.execute(sa.text(f"""
            CREATE UNIQUE INDEX {INDEX_NAME}
            ON customers (location_id, whatsapp_number)
            WHERE whatsapp_number IS NOT NULL AND deleted_at IS NULL
        """))
    finally:
        # Restore protection even if the duplicate check raised. Leaving RLS
        # off would expose every tenant's customers to every other tenant.
        op.execute(sa.text('ALTER TABLE "customers" ENABLE ROW LEVEL SECURITY'))
        op.execute(sa.text('ALTER TABLE "customers" FORCE ROW LEVEL SECURITY'))


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    op.execute(sa.text(f"DROP INDEX IF EXISTS {INDEX_NAME}"))
