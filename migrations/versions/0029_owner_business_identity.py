"""Move business identity from locations to owners.

WHY
---
Business identity -- registered legal name, CIPC registration number, trading
name, business email -- was stored on `locations`. That is the wrong home for
it: a business has ONE legal identity regardless of how many locations it
operates. With identity on the location, a second branch would either duplicate
the company's CIPC number or strand it on branch one.

`locations.owner_id` is currently UNIQUE, so today this is a one-to-one move and
nothing changes functionally. The value is structural: when multi-location
arrives, identity and legal acceptance stay with the business.

Legal acceptance moves for the same reason. A workshop accepts the Terms of
Service as a business, not per branch.

WHAT THIS DOES
--------------
1. Adds legal_name, business_registration_number, trading_name, business_email
   to `owners`.
2. Backfills them from the matching `locations` row.
3. Adds `owner_id` to `legal_acceptances` and backfills it via location.

WHAT IT DOES NOT DO
-------------------
It does not drop the old columns from `locations`. Dropping them is
irreversible and would break any read path not yet migrated; they are left in
place, unused by the new onboarding, to be removed in a later migration once
production has been observed running without them.

`locations.vat_number` is deliberately retained and NOT moved: VAT is billing
information, collected at the paywall rather than during onboarding.

RLS NOTE
--------
legal_acceptances keeps its location-keyed isolation policy. Rows are written
with BOTH owner_id and location_id so the existing policy continues to work
while the record is semantically owned by the business. Revisit if a location
is ever deleted independently of its owner.
"""
from alembic import op
import sqlalchemy as sa

revision = "0029_owner_business_identity"
down_revision = "0028_security_incidents"
branch_labels = None
depends_on = None

OWNER_COLUMNS = {
    "legal_name": sa.String(255),
    "business_registration_number": sa.String(64),
    "trading_name": sa.String(255),
    "business_email": sa.String(255),
}


def _columns(inspector, table):
    try:
        return {c["name"] for c in inspector.get_columns(table)}
    except Exception:
        return set()


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())

    if "owners" in tables:
        existing = _columns(inspector, "owners")
        for name, coltype in OWNER_COLUMNS.items():
            if name not in existing:
                op.add_column("owners", sa.Column(name, coltype, nullable=True))

    if "legal_acceptances" in tables and "owner_id" not in _columns(inspector, "legal_acceptances"):
        op.add_column("legal_acceptances", sa.Column("owner_id", sa.Integer, nullable=True))

    # Postal code completes the workshop address captured during onboarding.
    if "locations" in tables and "postal_code" not in _columns(inspector, "locations"):
        op.add_column("locations", sa.Column("postal_code", sa.String(20), nullable=True))

    if bind.dialect.name != "postgresql":
        return

    # legal_acceptances has FORCE ROW LEVEL SECURITY and migrations run as the
    # table owner with no app.location_id set, so the owner_id backfill below
    # would silently update zero rows. Disable RLS for the repair, restore it
    # immediately after. See 0026 for the full explanation.
    has_legal = "legal_acceptances" in tables
    if has_legal:
        op.execute(sa.text('ALTER TABLE "legal_acceptances" DISABLE ROW LEVEL SECURITY'))

    location_columns = _columns(sa.inspect(bind), "locations")

    # Backfill owner identity from the owner's single location.
    backfills = [
        ("legal_name", "legal_name"),
        ("business_registration_number", "business_registration_number"),
        ("trading_name", "trading_name"),
        ("business_email", "contact_email"),
    ]
    for owner_col, location_col in backfills:
        if location_col not in location_columns:
            continue
        op.execute(sa.text(f"""
            UPDATE owners AS o
            SET {owner_col} = l.{location_col}
            FROM locations AS l
            WHERE l.owner_id = o.id
              AND o.{owner_col} IS NULL
              AND l.{location_col} IS NOT NULL
        """))

    # Backfill owner_id on existing acceptance records via their location.
    if has_legal:
        op.execute(sa.text("""
            UPDATE legal_acceptances AS la
            SET owner_id = l.owner_id
            FROM locations AS l
            WHERE la.location_id = l.id
              AND la.owner_id IS NULL
        """))
        # Restore protection: leaving RLS off would expose every tenant's
        # acceptance records to every other tenant.
        op.execute(sa.text('ALTER TABLE "legal_acceptances" ENABLE ROW LEVEL SECURITY'))
        op.execute(sa.text('ALTER TABLE "legal_acceptances" FORCE ROW LEVEL SECURITY'))


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if "legal_acceptances" in set(inspector.get_table_names()):
        if "owner_id" in _columns(inspector, "legal_acceptances"):
            op.drop_column("legal_acceptances", "owner_id")

    existing = _columns(inspector, "owners")
    for name in OWNER_COLUMNS:
        if name in existing:
            op.drop_column("owners", name)
