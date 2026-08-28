"""Owner -> Location foundation for the canonical runtime architecture.

The active application uses only owners and locations for business scope.
Historical migrations are kept separately under the migrations package.
"""
from .query import _run


SCOPED_TABLES = [
    "customers","vehicles","services","bookings","reminder_campaigns",
    "communication_logs","service_prices","chatbot_messages","booking_inquiries",
    "chatbot_usage_daily","chatbot_usage_monthly","credential_audit","audit_logs",
    "automation_rules","scheduled_jobs","automation_logs","failed_jobs",
    "billing_records","usage_daily","onboarding_sessions","onboarding_answers",
    "onboarding_state","feature_flags",
]


def _add_column(connection, backend, table, column, definition):
    if backend == "postgres":
        cur = connection.cursor()
        try:
            cur.execute(
                """SELECT 1 FROM information_schema.columns
                   WHERE table_schema=current_schema() AND table_name=%s AND column_name=%s""",
                (table, column),
            )
            exists = cur.fetchone() is not None
        finally:
            cur.close()
    else:
        exists = any(r[1] == column for r in connection.execute(f"PRAGMA table_info({table})").fetchall())
    if not exists:
        _run(connection, backend, f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def ensure_owner_location_foundation(connection, backend):
    pk = "SERIAL PRIMARY KEY" if backend == "postgres" else "INTEGER PRIMARY KEY AUTOINCREMENT"
    boolean = "BOOLEAN" if backend == "postgres" else "INTEGER"

    _run(connection, backend, f"""
        CREATE TABLE IF NOT EXISTS owners (
            id {pk},
            user_id INTEGER UNIQUE,
            name TEXT,
            email TEXT,
            active {boolean} DEFAULT TRUE,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    _run(connection, backend, f"""
        CREATE TABLE IF NOT EXISTS locations (
            id {pk},
            owner_id INTEGER UNIQUE NOT NULL REFERENCES owners(id),
            name TEXT NOT NULL,
            slug TEXT,
            contact_email TEXT,
            contact_phone TEXT,
            industry TEXT DEFAULT 'workshop',
            subscription_status TEXT DEFAULT 'active',
            subscription_start TEXT,
            subscription_end TEXT,
            setup_fee REAL DEFAULT 0,
            public_base_url TEXT,
            inbound_webhook_token TEXT,
            plan_code TEXT DEFAULT 'basic',
            user_limit INTEGER DEFAULT 2,
            automation_enabled {boolean} DEFAULT FALSE,
            chatbot_enabled {boolean} DEFAULT FALSE,
            reporting_enabled {boolean} DEFAULT FALSE,
            custom_integrations_enabled {boolean} DEFAULT FALSE,
            priority_support_enabled {boolean} DEFAULT FALSE,
            monthly_base_price REAL DEFAULT 0,
            monthly_message_limit INTEGER DEFAULT 2000,
            messages_used INTEGER DEFAULT 0,
            overage_price_per_message REAL DEFAULT 0.5,
            billing_day TEXT,
            active {boolean} DEFAULT TRUE,
            operating_hours_json TEXT,
            notification_preferences_json TEXT,
            trading_name TEXT,
            business_registration_number TEXT,
            vat_number TEXT,
            website TEXT,
            physical_address TEXT,
            postal_address TEXT,
            province TEXT,
            city TEXT,
            workshop_type TEXT,
            timezone TEXT DEFAULT 'Africa/Johannesburg',
            currency TEXT DEFAULT 'ZAR',
            language TEXT DEFAULT 'en',
            description TEXT,
            daily_capacity INTEGER DEFAULT 12,
            public_booking_enabled {boolean} DEFAULT TRUE,
            access_locked {boolean} DEFAULT FALSE,
            access_locked_reason TEXT,
            access_locked_at TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)

    # Existing SQLite files created before these columns existed won't have
    # them from the CREATE TABLE above (CREATE TABLE IF NOT EXISTS is a
    # no-op against an already-present table) -- add_column for the same
    # reason the block below does it for users/SCOPED_TABLES.
    _add_column(connection, backend, "locations", "access_locked", f"{boolean} DEFAULT FALSE" if backend == "postgres" else f"{boolean} DEFAULT 0")
    _add_column(connection, backend, "locations", "access_locked_reason", "TEXT")
    _add_column(connection, backend, "locations", "access_locked_at", "TEXT")

    # Canonical identity/scope columns used by the active application.
    # Business identity columns on owners. Added here (rather than only in the
    # alembic migration) so a fresh SQLite development database gets them too.
    for column in ("legal_name", "business_registration_number", "trading_name", "business_email"):
        _add_column(connection, backend, "owners", column, "TEXT")

    # Postal code completes the workshop address captured during onboarding.
    _add_column(connection, backend, "locations", "postal_code", "TEXT")

    _add_column(connection, backend, "users", "owner_id", "INTEGER")
    _add_column(connection, backend, "users", "location_id", "INTEGER")
    for table in SCOPED_TABLES:
        _add_column(connection, backend, table, "location_id", "INTEGER")

