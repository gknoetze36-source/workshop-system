"""Step 5: enforce Owner -> Location scope for core customer data."""
from alembic import op
import sqlalchemy as sa

revision = "0017_step5_core_location_security"
down_revision = "0016_owner_location_foundation"
branch_labels = None
depends_on = None

CORE_TABLES = ("customers", "vehicles", "bookings", "booking_inquiries", "notes")


def _has_column(inspector, table, column):
    return column in {c["name"] for c in inspector.get_columns(table)}


def _has_fk(inspector, table, local_column, referred_table):
    for fk in inspector.get_foreign_keys(table):
        if local_column in (fk.get("constrained_columns") or []) and fk.get("referred_table") == referred_table:
            return True
    return False


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())

    if "notes" in tables:
        if not _has_column(inspector, "notes", "location_id"):
            op.add_column("notes", sa.Column("location_id", sa.Integer(), nullable=True))

        op.execute(sa.text(
            "UPDATE notes n SET location_id = u.location_id "
            "FROM users u WHERE n.location_id IS NULL "
            "AND n.created_by_user_id = u.id AND u.location_id IS NOT NULL"
        ))

        unresolved = bind.execute(sa.text(
            "SELECT COUNT(*) FROM notes WHERE location_id IS NULL"
        )).scalar_one()
        if unresolved:
            raise RuntimeError(
                f"0017 cannot safely migrate {unresolved} note(s): "
                "no owner/location can be proven from the note author."
            )

        op.alter_column("notes", "location_id", nullable=False)

    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())

    for table in CORE_TABLES:
        if table not in tables:
            continue
        if not _has_column(inspector, table, "location_id"):
            raise RuntimeError(f"{table}.location_id is required by the Step 5 contract")
        if not _has_fk(inspector, table, "location_id", "locations"):
            op.create_foreign_key(
                f"fk_{table}_location", table, "locations",
                ["location_id"], ["id"], ondelete="CASCADE"
            )

    op.execute(sa.text("""
        CREATE OR REPLACE FUNCTION phanta_validate_customer_location()
        RETURNS trigger AS $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM customers
                WHERE id = NEW.customer_id
                  AND location_id = NEW.location_id
            ) THEN
                RAISE EXCEPTION 'customer does not belong to booking/vehicle location';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """))

    op.execute(sa.text("""
        DROP TRIGGER IF EXISTS vehicles_customer_location_guard ON vehicles;
        CREATE TRIGGER vehicles_customer_location_guard
        BEFORE INSERT OR UPDATE OF customer_id, location_id ON vehicles
        FOR EACH ROW EXECUTE FUNCTION phanta_validate_customer_location();
    """))

    op.execute(sa.text("""
        DROP TRIGGER IF EXISTS bookings_customer_location_guard ON bookings;
        CREATE TRIGGER bookings_customer_location_guard
        BEFORE INSERT OR UPDATE OF customer_id, location_id ON bookings
        FOR EACH ROW EXECUTE FUNCTION phanta_validate_customer_location();
    """))

    op.execute(sa.text("""
        CREATE OR REPLACE FUNCTION phanta_validate_booking_vehicle_location()
        RETURNS trigger AS $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM vehicles
                WHERE id = NEW.vehicle_id
                  AND location_id = NEW.location_id
                  AND customer_id = NEW.customer_id
            ) THEN
                RAISE EXCEPTION 'vehicle does not belong to booking customer/location';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """))

    op.execute(sa.text("""
        DROP TRIGGER IF EXISTS bookings_vehicle_location_guard ON bookings;
        CREATE TRIGGER bookings_vehicle_location_guard
        BEFORE INSERT OR UPDATE OF vehicle_id, customer_id, location_id ON bookings
        FOR EACH ROW EXECUTE FUNCTION phanta_validate_booking_vehicle_location();
    """))

    for table in CORE_TABLES:
        if table not in tables:
            continue
        op.execute(sa.text(f'ALTER TABLE "{table}" ENABLE ROW LEVEL SECURITY'))
        op.execute(sa.text(f'ALTER TABLE "{table}" FORCE ROW LEVEL SECURITY'))
        op.execute(sa.text(f'DROP POLICY IF EXISTS "{table}_tenant_isolation" ON "{table}"'))
        op.execute(sa.text(f'DROP POLICY IF EXISTS "{table}_location_isolation" ON "{table}"'))
        op.execute(sa.text(f'DROP POLICY IF EXISTS "{table}_platform_admin_read" ON "{table}"'))
        policy = f'''
            CREATE POLICY "{table}_location_isolation"
            ON "{table}"
            USING (
                location_id = NULLIF(current_setting('app.location_id', true), '')::integer
                OR current_setting('app.platform_admin', true) = '1'
            )
            WITH CHECK (
                location_id = NULLIF(current_setting('app.location_id', true), '')::integer
            )
        '''
        op.execute(sa.text(policy))

    indexes = [
        ("ix_vehicles_location_customer_step5", "vehicles(location_id, customer_id)"),
        ("ix_bookings_location_customer_step5", "bookings(location_id, customer_id)"),
        ("ix_bookings_location_vehicle_step5", "bookings(location_id, vehicle_id)"),
        ("ix_booking_inquiries_location_step5", "booking_inquiries(location_id, updated_at)"),
        ("ix_notes_location_subject_step5", "notes(location_id, subject_type, subject_id, created_at)"),
    ]
    for name, expression in indexes:
        op.execute(sa.text(f"CREATE INDEX IF NOT EXISTS {name} ON {expression}"))


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    for table in CORE_TABLES:
        op.execute(sa.text(f'DROP POLICY IF EXISTS "{table}_location_isolation" ON "{table}"'))
    op.execute(sa.text("DROP TRIGGER IF EXISTS bookings_vehicle_location_guard ON bookings"))
    op.execute(sa.text("DROP TRIGGER IF EXISTS bookings_customer_location_guard ON bookings"))
    op.execute(sa.text("DROP TRIGGER IF EXISTS vehicles_customer_location_guard ON vehicles"))
    op.execute(sa.text("DROP FUNCTION IF EXISTS phanta_validate_booking_vehicle_location()"))
    op.execute(sa.text("DROP FUNCTION IF EXISTS phanta_validate_customer_location()"))
