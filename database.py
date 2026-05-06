import csv
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

BASE_DIR = Path(__file__).resolve().parent
PRIMARY_SQLITE_PATH = os.environ.get("SQLITE_PATH") or str(BASE_DIR / "database.db")
DEFAULT_FRANCHISE_NAME = os.environ.get("DEFAULT_FRANCHISE_NAME", "Main Workshop Group")
BOOKINGS_CSV_PATH = BASE_DIR / "bookings.csv"


def utc_now():
    return datetime.utcnow().replace(microsecond=0).isoformat()


def slugify(value):
    text = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
    return text.strip("-") or "item"


def parse_any_date(value):
    text = str(value or "").strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def iso_date(value):
    parsed = parse_any_date(value)
    return parsed.strftime("%Y-%m-%d") if parsed else ""


def classify_service_level(service_name):
    text = str(service_name or "").lower()
    if "major" in text:
        return "Major"
    if "minor" in text:
        return "Minor"
    return "General"


def get_connection():
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        import psycopg2
        import psycopg2.extras

        connection = psycopg2.connect(database_url)
        connection.autocommit = False
        return connection, "postgres"

    connection = sqlite3.connect(PRIMARY_SQLITE_PATH)
    connection.row_factory = sqlite3.Row
    return connection, "sqlite"


def _adapt_query(query, backend):
    if backend == "sqlite":
        return query.replace("%s", "?")
    return query


def _get_cursor(connection, backend):
    if backend == "postgres":
        import psycopg2.extras

        return connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return connection.cursor()


def _run(connection, backend, query, args=(), one=False):
    cursor = _get_cursor(connection, backend)
    try:
        cursor.execute(_adapt_query(query, backend), args)
        if cursor.description:
            rows = [dict(row) for row in cursor.fetchall()]
            return rows[0] if one and rows else (None if one else rows)
        connection.commit()
        return None
    finally:
        cursor.close()


def query_db(query, args=(), one=False):
    connection, backend = get_connection()
    try:
        return _run(connection, backend, query, args, one=one)
    finally:
        connection.close()


def execute_db(query, args=()):
    query_db(query, args=args, one=False)


def _get_columns(connection, backend, table_name):
    if backend == "postgres":
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = %s
                """,
                (table_name,),
            )
            return {row[0] for row in cursor.fetchall()}
        finally:
            cursor.close()

    cursor = connection.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cursor.fetchall()}


def _create_tables(connection, backend):
    primary_key = "SERIAL PRIMARY KEY" if backend == "postgres" else "INTEGER PRIMARY KEY AUTOINCREMENT"
    integer_boolean = "BOOLEAN" if backend == "postgres" else "INTEGER"

    for query in [
        f"""
        CREATE TABLE IF NOT EXISTS franchises (
            id {primary_key},
            name TEXT NOT NULL,
            slug TEXT,
            contact_email TEXT,
            contact_phone TEXT,
            notes TEXT,
            public_base_url TEXT,
            inbound_webhook_token TEXT,
            plan_code TEXT DEFAULT 'basic',
            branch_limit INTEGER DEFAULT 1,
            user_limit INTEGER DEFAULT 2,
            automation_enabled {integer_boolean} DEFAULT 0,
            chatbot_enabled {integer_boolean} DEFAULT 0,
            reporting_enabled {integer_boolean} DEFAULT 0,
            custom_integrations_enabled {integer_boolean} DEFAULT 0,
            priority_support_enabled {integer_boolean} DEFAULT 0,
            monthly_base_price REAL DEFAULT 0,
            monthly_message_limit INTEGER DEFAULT 2000,
            overage_price_per_message REAL DEFAULT 0.5,
            billing_day TEXT,
            active {integer_boolean} DEFAULT 1,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS branches (
            id {primary_key},
            franchise_id INTEGER,
            name TEXT NOT NULL,
            slug TEXT,
            code TEXT,
            location TEXT,
            contact_email TEXT,
            contact_phone TEXT,
            daily_capacity INTEGER DEFAULT 12,
            public_booking_enabled {integer_boolean} DEFAULT 1,
            active {integer_boolean} DEFAULT 1,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS users (
            id {primary_key},
            username TEXT,
            password TEXT,
            password_hash TEXT,
            full_name TEXT,
            email TEXT,
            phone TEXT,
            branch TEXT,
            company TEXT,
            role TEXT,
            franchise_id INTEGER,
            branch_id INTEGER,
            active {integer_boolean} DEFAULT 1,
            must_reset_password {integer_boolean} DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS bookings (
            id {primary_key},
            booking_reference TEXT,
            franchise_id INTEGER,
            branch_id INTEGER,
            company TEXT,
            branch TEXT,
            first_name TEXT,
            surname TEXT,
            customer_email TEXT,
            phone TEXT,
            preferred_contact_method TEXT,
            make TEXT,
            model TEXT,
            vehicle_year TEXT,
            fuel_type TEXT,
            vehicle_vin TEXT,
            service TEXT,
            service_level TEXT,
            current_mileage TEXT,
            scheduled_date TEXT,
            date TEXT,
            status TEXT,
            service_due_date TEXT,
            work_to_be_done TEXT,
            public_notes TEXT,
            internal_notes TEXT,
            source TEXT,
            quote_declined TEXT,
            contacted TEXT,
            missed_followup_count INTEGER DEFAULT 0,
            last_missed_followup_at TEXT,
            last_customer_reply_at TEXT,
            whatsapp_opt_in {integer_boolean} DEFAULT 0,
            privacy_consent_at TEXT,
            reminder_opt_in {integer_boolean} DEFAULT 1,
            completed_at TEXT,
            created_at TEXT,
            updated_at TEXT,
            legacy_source_key TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS reminder_campaigns (
            id {primary_key},
            booking_id INTEGER,
            franchise_id INTEGER,
            branch_id INTEGER,
            reminder_kind TEXT,
            due_date TEXT,
            campaign_round INTEGER,
            scheduled_for TEXT,
            status TEXT,
            message_subject TEXT,
            message_body TEXT,
            last_channel_used TEXT,
            send_count INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT,
            sent_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS communication_logs (
            id {primary_key},
            booking_id INTEGER,
            reminder_id INTEGER,
            franchise_id INTEGER,
            branch_id INTEGER,
            user_id INTEGER,
            channel TEXT,
            recipient TEXT,
            subject TEXT,
            body TEXT,
            status TEXT,
            external_target TEXT,
            created_at TEXT,
            sent_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS service_prices (
            id {primary_key},
            franchise_id INTEGER,
            branch_id INTEGER,
            service_name TEXT,
            service_category TEXT,
            price_amount REAL DEFAULT 0,
            active {integer_boolean} DEFAULT 1,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS chatbot_messages (
            id {primary_key},
            franchise_id INTEGER,
            branch_id INTEGER,
            customer_name TEXT,
            customer_phone TEXT,
            customer_email TEXT,
            channel TEXT,
            direction TEXT,
            message_text TEXT,
            suggested_service TEXT,
            matched_price REAL,
            status TEXT,
            processed {integer_boolean} DEFAULT 0,
            privacy_notice_sent {integer_boolean} DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS booking_inquiries (
            id {primary_key},
            franchise_id INTEGER,
            branch_id INTEGER,
            booking_id INTEGER,
            customer_name TEXT,
            customer_phone TEXT,
            customer_email TEXT,
            source_channel TEXT,
            user_state TEXT,
            service_type TEXT,
            last_message_text TEXT,
            last_user_interaction_at TEXT,
            last_followup_at TEXT,
            followup_stage INTEGER DEFAULT 0,
            next_followup_at TEXT,
            followups_sent_count INTEGER DEFAULT 0,
            replies_after_followup_count INTEGER DEFAULT 0,
            bookings_from_followups_count INTEGER DEFAULT 0,
            stop_reason TEXT,
            declined {integer_boolean} DEFAULT 0,
            closed_at TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS inquiry_followup_events (
            id {primary_key},
            inquiry_id INTEGER,
            followup_stage INTEGER,
            channel TEXT,
            message_subject TEXT,
            message_body TEXT,
            status TEXT,
            sent_at TEXT,
            created_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS chatbot_usage_daily (
            id {primary_key},
            franchise_id INTEGER,
            usage_date TEXT,
            message_count INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS chatbot_usage_monthly (
            id {primary_key},
            franchise_id INTEGER,
            usage_month TEXT,
            message_count INTEGER DEFAULT 0,
            message_limit INTEGER DEFAULT 2000,
            extra_messages INTEGER DEFAULT 0,
            base_price REAL DEFAULT 0,
            overage_price REAL DEFAULT 0.5,
            overage_cost REAL DEFAULT 0,
            total_due REAL DEFAULT 0,
            payment_status TEXT DEFAULT 'Unpaid',
            paid_at TEXT,
            payment_reference TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS credential_audit (
            id {primary_key},
            user_id INTEGER,
            username TEXT,
            franchise_id INTEGER,
            actor_user_id INTEGER,
            event_type TEXT,
            note TEXT,
            created_at TEXT
        )
        """,
    ]:
        _run(connection, backend, query)


def _ensure_columns(connection, backend):
    desired_columns = {
        "franchises": {
            "slug": "TEXT",
            "contact_email": "TEXT",
            "contact_phone": "TEXT",
            "notes": "TEXT",
            "public_base_url": "TEXT",
            "inbound_webhook_token": "TEXT",
            "plan_code": "TEXT DEFAULT 'basic'",
            "branch_limit": "INTEGER DEFAULT 1",
            "user_limit": "INTEGER DEFAULT 2",
            "automation_enabled": "BOOLEAN DEFAULT 0" if backend == "postgres" else "INTEGER DEFAULT 0",
            "chatbot_enabled": "BOOLEAN DEFAULT 0" if backend == "postgres" else "INTEGER DEFAULT 0",
            "reporting_enabled": "BOOLEAN DEFAULT 0" if backend == "postgres" else "INTEGER DEFAULT 0",
            "custom_integrations_enabled": "BOOLEAN DEFAULT 0" if backend == "postgres" else "INTEGER DEFAULT 0",
            "priority_support_enabled": "BOOLEAN DEFAULT 0" if backend == "postgres" else "INTEGER DEFAULT 0",
            "monthly_base_price": "REAL DEFAULT 0",
            "monthly_message_limit": "INTEGER DEFAULT 2000",
            "overage_price_per_message": "REAL DEFAULT 0.5",
            "billing_day": "TEXT",
            "active": "BOOLEAN DEFAULT 1" if backend == "postgres" else "INTEGER DEFAULT 1",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "branches": {
            "franchise_id": "INTEGER",
            "slug": "TEXT",
            "code": "TEXT",
            "location": "TEXT",
            "contact_email": "TEXT",
            "contact_phone": "TEXT",
            "daily_capacity": "INTEGER DEFAULT 12",
            "public_booking_enabled": "BOOLEAN DEFAULT 1" if backend == "postgres" else "INTEGER DEFAULT 1",
            "active": "BOOLEAN DEFAULT 1" if backend == "postgres" else "INTEGER DEFAULT 1",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "users": {
            "password_hash": "TEXT",
            "full_name": "TEXT",
            "email": "TEXT",
            "phone": "TEXT",
            "company": "TEXT",
            "franchise_id": "INTEGER",
            "branch_id": "INTEGER",
            "active": "BOOLEAN DEFAULT 1" if backend == "postgres" else "INTEGER DEFAULT 1",
            "must_reset_password": "BOOLEAN DEFAULT 0" if backend == "postgres" else "INTEGER DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "bookings": {
            "booking_reference": "TEXT",
            "franchise_id": "INTEGER",
            "branch_id": "INTEGER",
            "company": "TEXT",
            "branch": "TEXT",
            "first_name": "TEXT",
            "surname": "TEXT",
            "customer_email": "TEXT",
            "phone": "TEXT",
            "preferred_contact_method": "TEXT",
            "make": "TEXT",
            "model": "TEXT",
            "vehicle_year": "TEXT",
            "fuel_type": "TEXT",
            "vehicle_vin": "TEXT",
            "service": "TEXT",
            "service_level": "TEXT",
            "current_mileage": "TEXT",
            "scheduled_date": "TEXT",
            "date": "TEXT",
            "work_to_be_done": "TEXT",
            "public_notes": "TEXT",
            "internal_notes": "TEXT",
            "reminder_opt_in": "BOOLEAN DEFAULT 1" if backend == "postgres" else "INTEGER DEFAULT 1",
            "completed_at": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
            "legacy_source_key": "TEXT",
            "status": "TEXT DEFAULT 'Pending'",
            "service_due_date": "TEXT",
            "source": "TEXT DEFAULT 'Website'",
            "quote_declined": "TEXT DEFAULT 'No'",
            "contacted": "TEXT DEFAULT 'No'",
            "missed_followup_count": "INTEGER DEFAULT 0",
            "last_missed_followup_at": "TEXT",
            "last_customer_reply_at": "TEXT",
            "whatsapp_opt_in": "BOOLEAN DEFAULT 0" if backend == "postgres" else "INTEGER DEFAULT 0",
            "privacy_consent_at": "TEXT",
        },
        "reminder_campaigns": {
            "booking_id": "INTEGER",
            "franchise_id": "INTEGER",
            "branch_id": "INTEGER",
            "reminder_kind": "TEXT",
            "due_date": "TEXT",
            "campaign_round": "INTEGER",
            "scheduled_for": "TEXT",
            "status": "TEXT",
            "message_subject": "TEXT",
            "message_body": "TEXT",
            "last_channel_used": "TEXT",
            "send_count": "INTEGER DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
            "sent_at": "TEXT",
        },
        "communication_logs": {
            "booking_id": "INTEGER",
            "reminder_id": "INTEGER",
            "franchise_id": "INTEGER",
            "branch_id": "INTEGER",
            "user_id": "INTEGER",
            "channel": "TEXT",
            "recipient": "TEXT",
            "subject": "TEXT",
            "body": "TEXT",
            "status": "TEXT",
            "external_target": "TEXT",
            "created_at": "TEXT",
            "sent_at": "TEXT",
        },
        "service_prices": {
            "franchise_id": "INTEGER",
            "branch_id": "INTEGER",
            "service_name": "TEXT",
            "service_category": "TEXT",
            "price_amount": "REAL DEFAULT 0",
            "active": "BOOLEAN DEFAULT 1" if backend == "postgres" else "INTEGER DEFAULT 1",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "chatbot_messages": {
            "franchise_id": "INTEGER",
            "branch_id": "INTEGER",
            "customer_name": "TEXT",
            "customer_phone": "TEXT",
            "customer_email": "TEXT",
            "channel": "TEXT",
            "direction": "TEXT",
            "message_text": "TEXT",
            "suggested_service": "TEXT",
            "matched_price": "REAL",
            "status": "TEXT",
            "processed": "BOOLEAN DEFAULT 0" if backend == "postgres" else "INTEGER DEFAULT 0",
            "privacy_notice_sent": "BOOLEAN DEFAULT 0" if backend == "postgres" else "INTEGER DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "booking_inquiries": {
            "franchise_id": "INTEGER",
            "branch_id": "INTEGER",
            "booking_id": "INTEGER",
            "customer_name": "TEXT",
            "customer_phone": "TEXT",
            "customer_email": "TEXT",
            "source_channel": "TEXT",
            "user_state": "TEXT",
            "service_type": "TEXT",
            "last_message_text": "TEXT",
            "last_user_interaction_at": "TEXT",
            "last_followup_at": "TEXT",
            "followup_stage": "INTEGER DEFAULT 0",
            "next_followup_at": "TEXT",
            "followups_sent_count": "INTEGER DEFAULT 0",
            "replies_after_followup_count": "INTEGER DEFAULT 0",
            "bookings_from_followups_count": "INTEGER DEFAULT 0",
            "stop_reason": "TEXT",
            "declined": "BOOLEAN DEFAULT 0" if backend == "postgres" else "INTEGER DEFAULT 0",
            "closed_at": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "inquiry_followup_events": {
            "inquiry_id": "INTEGER",
            "followup_stage": "INTEGER",
            "channel": "TEXT",
            "message_subject": "TEXT",
            "message_body": "TEXT",
            "status": "TEXT",
            "sent_at": "TEXT",
            "created_at": "TEXT",
        },
        "chatbot_usage_daily": {
            "franchise_id": "INTEGER",
            "usage_date": "TEXT",
            "message_count": "INTEGER DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "chatbot_usage_monthly": {
            "franchise_id": "INTEGER",
            "usage_month": "TEXT",
            "message_count": "INTEGER DEFAULT 0",
            "message_limit": "INTEGER DEFAULT 2000",
            "extra_messages": "INTEGER DEFAULT 0",
            "base_price": "REAL DEFAULT 0",
            "overage_price": "REAL DEFAULT 0.5",
            "overage_cost": "REAL DEFAULT 0",
            "total_due": "REAL DEFAULT 0",
            "payment_status": "TEXT DEFAULT 'Unpaid'",
            "paid_at": "TEXT",
            "payment_reference": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "credential_audit": {
            "user_id": "INTEGER",
            "username": "TEXT",
            "franchise_id": "INTEGER",
            "actor_user_id": "INTEGER",
            "event_type": "TEXT",
            "note": "TEXT",
            "created_at": "TEXT",
        },
    }

    for table_name, columns in desired_columns.items():
        existing = _get_columns(connection, backend, table_name)
        for column_name, definition in columns.items():
            if column_name not in existing:
                _run(connection, backend, f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def _ensure_indexes(connection, backend):
    index_queries = [
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_franchises_slug ON franchises(slug)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_branches_franchise_slug ON branches(franchise_id, slug)",
        "CREATE INDEX IF NOT EXISTS idx_branches_franchise ON branches(franchise_id)",
        "CREATE INDEX IF NOT EXISTS idx_users_franchise ON users(franchise_id)",
        "CREATE INDEX IF NOT EXISTS idx_users_branch ON users(branch_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_bookings_reference ON bookings(booking_reference)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_bookings_legacy_source ON bookings(legacy_source_key)",
        "CREATE INDEX IF NOT EXISTS idx_bookings_scope ON bookings(franchise_id, branch_id, scheduled_date)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_reminder_unique_round ON reminder_campaigns(booking_id, reminder_kind, campaign_round)",
        "CREATE INDEX IF NOT EXISTS idx_communication_logs_scope ON communication_logs(franchise_id, branch_id, channel)",
        "CREATE INDEX IF NOT EXISTS idx_service_prices_scope ON service_prices(franchise_id, branch_id, service_name)",
        "CREATE INDEX IF NOT EXISTS idx_chatbot_messages_scope ON chatbot_messages(franchise_id, branch_id, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_booking_inquiries_scope ON booking_inquiries(franchise_id, branch_id, user_state, next_followup_at)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_booking_inquiries_contact ON booking_inquiries(franchise_id, branch_id, customer_phone, source_channel)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_inquiry_followup_events_unique ON inquiry_followup_events(inquiry_id, followup_stage)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_chatbot_usage_daily_scope ON chatbot_usage_daily(franchise_id, usage_date)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_chatbot_usage_monthly_scope ON chatbot_usage_monthly(franchise_id, usage_month)",
        "CREATE INDEX IF NOT EXISTS idx_credential_audit_scope ON credential_audit(franchise_id, created_at)",
    ]
    for query in index_queries:
        _run(connection, backend, query)


def _seed_plan_defaults(connection, backend):
    now = utc_now()
    plans = {
        "basic": {"branch_limit": 1, "user_limit": 2, "automation_enabled": 0, "chatbot_enabled": 0, "reporting_enabled": 0, "custom_integrations_enabled": 0, "priority_support_enabled": 0},
        "growth": {"branch_limit": 5, "user_limit": 10, "automation_enabled": 1, "chatbot_enabled": 1, "reporting_enabled": 1, "custom_integrations_enabled": 0, "priority_support_enabled": 0},
        "premium": {"branch_limit": 999999, "user_limit": 999999, "automation_enabled": 1, "chatbot_enabled": 1, "reporting_enabled": 1, "custom_integrations_enabled": 1, "priority_support_enabled": 1},
    }
    franchises = _run(connection, backend, "SELECT * FROM franchises ORDER BY id") or []
    for franchise in franchises:
        plan_code = (franchise.get("plan_code") or "basic").lower()
        plan = plans.get(plan_code, plans["basic"])
        _run(
            connection,
            backend,
            """
            UPDATE franchises
            SET plan_code=%s,
                branch_limit=COALESCE(NULLIF(branch_limit, 0), %s),
                user_limit=COALESCE(NULLIF(user_limit, 0), %s),
                automation_enabled=COALESCE(automation_enabled, %s),
                chatbot_enabled=COALESCE(chatbot_enabled, %s),
                reporting_enabled=COALESCE(reporting_enabled, %s),
                custom_integrations_enabled=COALESCE(custom_integrations_enabled, %s),
                priority_support_enabled=COALESCE(priority_support_enabled, %s),
                monthly_message_limit=COALESCE(NULLIF(monthly_message_limit, 0), 2000),
                overage_price_per_message=COALESCE(overage_price_per_message, 0.5),
                billing_day=COALESCE(billing_day, 'month_end'),
                updated_at=%s
            WHERE id=%s
            """,
            (
                plan_code,
                plan["branch_limit"],
                plan["user_limit"],
                plan["automation_enabled"],
                plan["chatbot_enabled"],
                plan["reporting_enabled"],
                plan["custom_integrations_enabled"],
                plan["priority_support_enabled"],
                now,
                franchise["id"],
            ),
        )


def _deduplicate_users(connection, backend):
    users = _run(connection, backend, "SELECT * FROM users ORDER BY id") or []
    seen = {}
    for user in users:
        original_username = (user.get("username") or "").strip()
        if not original_username:
            original_username = f"user-{user['id']}"
            _run(connection, backend, "UPDATE users SET username=%s WHERE id=%s", (original_username, user["id"]))

        username_key = original_username.lower()
        if username_key not in seen:
            seen[username_key] = user["id"]
            continue

        original_id = seen[username_key]
        original = _run(connection, backend, "SELECT * FROM users WHERE id=%s", (original_id,), one=True)
        if (
            original
            and (original.get("branch") or "") == (user.get("branch") or "")
            and (original.get("role") or "") == (user.get("role") or "")
            and (original.get("password") or "") == (user.get("password") or "")
        ):
            _run(connection, backend, "DELETE FROM users WHERE id=%s", (user["id"],))
            continue

        candidate = f"{original_username}-{user['id']}"
        suffix = 2
        while _run(connection, backend, "SELECT id FROM users WHERE lower(username)=lower(%s)", (candidate,), one=True):
            candidate = f"{original_username}-{suffix}"
            suffix += 1
        _run(connection, backend, "UPDATE users SET username=%s WHERE id=%s", (candidate, user["id"]))


def _ensure_unique_username_index(connection, backend):
    _run(connection, backend, "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users(username)")


def _get_or_create_franchise(connection, backend, name, contact_email="", contact_phone=""):
    existing = _run(connection, backend, "SELECT * FROM franchises WHERE lower(name)=lower(%s)", (name,), one=True)
    if existing:
        return existing

    slug_base = slugify(name)
    slug = slug_base
    suffix = 2
    while _run(connection, backend, "SELECT id FROM franchises WHERE slug=%s", (slug,), one=True):
        slug = f"{slug_base}-{suffix}"
        suffix += 1

    now = utc_now()
    _run(
        connection,
        backend,
        """
        INSERT INTO franchises (name, slug, contact_email, contact_phone, active, created_at, updated_at)
        VALUES (%s, %s, %s, %s, 1, %s, %s)
        """,
        (name, slug, contact_email, contact_phone, now, now),
    )
    return _run(connection, backend, "SELECT * FROM franchises WHERE slug=%s", (slug,), one=True)


def _get_or_create_branch(connection, backend, franchise_id, name, contact_email="", contact_phone="", location=""):
    branch = _run(
        connection,
        backend,
        "SELECT * FROM branches WHERE franchise_id=%s AND lower(name)=lower(%s)",
        (franchise_id, name),
        one=True,
    )
    if branch:
        return branch

    slug_base = slugify(name)
    slug = slug_base
    suffix = 2
    while _run(
        connection,
        backend,
        "SELECT id FROM branches WHERE franchise_id=%s AND slug=%s",
        (franchise_id, slug),
        one=True,
    ):
        slug = f"{slug_base}-{suffix}"
        suffix += 1

    now = utc_now()
    _run(
        connection,
        backend,
        """
        INSERT INTO branches (
            franchise_id, name, slug, contact_email, contact_phone, location,
            public_booking_enabled, active, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, 1, 1, %s, %s)
        """,
        (franchise_id, name, slug, contact_email, contact_phone, location, now, now),
    )
    return _run(
        connection,
        backend,
        "SELECT * FROM branches WHERE franchise_id=%s AND slug=%s",
        (franchise_id, slug),
        one=True,
    )


def _migrate_legacy_users(connection, backend):
    from werkzeug.security import generate_password_hash

    franchise = _get_or_create_franchise(connection, backend, DEFAULT_FRANCHISE_NAME)
    legacy_users = _run(connection, backend, "SELECT * FROM users ORDER BY id") or []
    now = utc_now()

    for user in legacy_users:
        branch_name = (user.get("branch") or "").strip()
        legacy_role = (user.get("role") or "").strip().lower()
        username = (user.get("username") or f"user-{user['id']}").strip()
        company_name = (user.get("company") or franchise["name"]).strip() or franchise["name"]

        franchise_record = _get_or_create_franchise(connection, backend, company_name)
        branch_record = None
        if branch_name and branch_name.upper() not in {"ALL", "MAIN"}:
            branch_record = _get_or_create_branch(connection, backend, franchise_record["id"], branch_name)

        if legacy_role in {"super_admin", "franchise_admin", "reception"}:
            role = legacy_role
        elif legacy_role == "super_admin":
            role = "super_admin"
        elif legacy_role == "admin":
            role = "franchise_admin"
        else:
            role = "reception"

        legacy_password = (user.get("password") or "").strip()
        if not user.get("password_hash") and legacy_password:
            password_hash = generate_password_hash(user["password"])
        else:
            password_hash = user.get("password_hash") or ""

        must_reset_password = 1 if legacy_password in {"1234", "admin", "password", "123456"} else int(user.get("must_reset_password") or 0)

        full_name = user.get("full_name") or username.replace(".", " ").replace("_", " ").title()
        _run(
            connection,
            backend,
            """
            UPDATE users
            SET password_hash=%s,
                full_name=%s,
                company=%s,
                role=%s,
                franchise_id=%s,
                branch_id=%s,
                active=COALESCE(active, 1),
                must_reset_password=%s,
                created_at=COALESCE(created_at, %s),
                updated_at=%s
            WHERE id=%s
            """,
            (
                password_hash,
                full_name,
                franchise_record["name"],
                role,
                franchise_record["id"],
                branch_record["id"] if branch_record else None,
                must_reset_password,
                now,
                now,
                user["id"],
            ),
        )


def _ensure_super_admin(connection, backend):
    from werkzeug.security import generate_password_hash

    existing = _run(connection, backend, "SELECT id FROM users WHERE role='super_admin' LIMIT 1", one=True)
    if existing:
        return

    username = os.environ.get("SUPERADMIN_USERNAME", "superadmin")
    password = os.environ.get("SUPERADMIN_PASSWORD", "ChangeMeNow!2026")
    full_name = os.environ.get("SUPERADMIN_NAME", "Platform Super Admin")
    now = utc_now()

    _run(
        connection,
        backend,
        """
        INSERT INTO users (
            username, password, password_hash, full_name, role, active,
            must_reset_password, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, 'super_admin', 1, 1, %s, %s)
        """,
        (username, "", generate_password_hash(password), full_name, now, now),
    )


def _harden_default_credentials(connection, backend):
    from werkzeug.security import generate_password_hash

    weak_passwords = {"1234", "admin", "password", "123456", "ChangeMeNow!2026", "login1234"}
    users = _run(connection, backend, "SELECT * FROM users ORDER BY id") or []
    now = utc_now()
    for user in users:
        plaintext = (user.get("password") or "").strip()
        password_hash = user.get("password_hash") or ""
        must_reset = int(user.get("must_reset_password") or 0)

        matched_weak = plaintext in weak_passwords

        if plaintext:
            password_hash = generate_password_hash(plaintext) if not password_hash else password_hash
            plaintext = ""

        if matched_weak:
            must_reset = 1

        _run(
            connection,
            backend,
            "UPDATE users SET password=%s, password_hash=%s, must_reset_password=%s, updated_at=%s WHERE id=%s",
            (plaintext, password_hash, must_reset, now, user["id"]),
        )


def _migrate_legacy_bookings(connection, backend):
    bookings = _run(connection, backend, "SELECT * FROM bookings ORDER BY id") or []
    now = utc_now()
    for booking in bookings:
        updates = {}

        if not booking.get("scheduled_date") and booking.get("date"):
            updates["scheduled_date"] = iso_date(booking.get("date"))
        if not booking.get("date") and booking.get("scheduled_date"):
            updates["date"] = booking.get("scheduled_date")
        if not booking.get("status"):
            updates["status"] = "Pending"
        if not booking.get("source"):
            updates["source"] = "Legacy"
        if not booking.get("quote_declined"):
            updates["quote_declined"] = "No"
        if not booking.get("contacted"):
            updates["contacted"] = "No"
        if not booking.get("service_level") and booking.get("service"):
            updates["service_level"] = classify_service_level(booking.get("service"))
        if not booking.get("created_at"):
            updates["created_at"] = now
        updates["updated_at"] = now

        branch_name = (booking.get("branch") or "").strip()
        company_name = (booking.get("company") or DEFAULT_FRANCHISE_NAME).strip() or DEFAULT_FRANCHISE_NAME
        if company_name:
            franchise = _get_or_create_franchise(connection, backend, company_name)
            updates["franchise_id"] = booking.get("franchise_id") or franchise["id"]
            if branch_name:
                branch = _get_or_create_branch(connection, backend, franchise["id"], branch_name)
                updates["branch_id"] = booking.get("branch_id") or branch["id"]

        if updates:
            assignments = ", ".join(f"{column}=%s" for column in updates)
            _run(connection, backend, f"UPDATE bookings SET {assignments} WHERE id=%s", tuple(updates.values()) + (booking["id"],))


def _generate_booking_reference(connection, backend, scheduled_date):
    prefix = f"BK-{(scheduled_date or datetime.utcnow().strftime('%Y-%m-%d')).replace('-', '')}"
    existing = _run(
        connection,
        backend,
        "SELECT booking_reference FROM bookings WHERE booking_reference LIKE %s ORDER BY booking_reference DESC",
        (f"{prefix}-%",),
    ) or []
    next_number = len(existing) + 1
    while True:
        reference = f"{prefix}-{next_number:04d}"
        if not _run(connection, backend, "SELECT id FROM bookings WHERE booking_reference=%s", (reference,), one=True):
            return reference
        next_number += 1


def _import_csv_bookings(connection, backend):
    if not BOOKINGS_CSV_PATH.exists():
        return

    existing_bookings = _run(connection, backend, "SELECT COUNT(*) AS count FROM bookings", one=True)
    if existing_bookings and int(existing_bookings["count"] or 0) > 0:
        return

    franchise = _get_or_create_franchise(connection, backend, DEFAULT_FRANCHISE_NAME)
    now = utc_now()

    with BOOKINGS_CSV_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader, start=1):
            branch_name = (row.get("branch") or "").strip() or "Main Branch"
            branch = _get_or_create_branch(connection, backend, franchise["id"], branch_name)
            scheduled_date = iso_date(row.get("date")) or datetime.utcnow().strftime("%Y-%m-%d")
            legacy_source_key = quote_plus(f"{row.get('Timestamp', '')}-{row.get('phone', '')}-{index}")
            if _run(connection, backend, "SELECT id FROM bookings WHERE legacy_source_key=%s", (legacy_source_key,), one=True):
                continue

            source = "Website"
            preferred_contact = (row.get("Preferred Contact Method ") or "").strip() or "WhatsApp"
            public_notes = f"Preferred vehicle lookup: {(row.get('How would you like to identify your vehicle ') or '').strip()}".strip()
            if row.get("Supply your own parts"):
                public_notes = (
                    f"{public_notes}\nCustomer supplies parts: {row.get('Supply your own parts')}".strip()
                    if public_notes
                    else f"Customer supplies parts: {row.get('Supply your own parts')}"
                )

            booking_reference = _generate_booking_reference(connection, backend, scheduled_date)
            _run(
                connection,
                backend,
                """
                INSERT INTO bookings (
                    booking_reference, franchise_id, branch_id, company, branch,
                    first_name, surname, customer_email, phone, preferred_contact_method,
                    make, model, vehicle_year, fuel_type, vehicle_vin, service, service_level,
                    current_mileage, scheduled_date, date, status, work_to_be_done,
                    public_notes, source, quote_declined, contacted, reminder_opt_in,
                    created_at, updated_at, legacy_source_key
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
                """,
                (
                    booking_reference,
                    franchise["id"],
                    branch["id"],
                    franchise["name"],
                    branch["name"],
                    (row.get("first_name") or "").strip(),
                    (row.get("surname") or "").strip(),
                    (row.get("email") or row.get("Email Address") or "").strip(),
                    (row.get("phone") or "").strip(),
                    preferred_contact,
                    (row.get("make") or "").strip(),
                    (row.get("model") or "").strip(),
                    (row.get("year") or "").strip(),
                    (row.get("Fuel type ") or "").strip(),
                    (row.get("Enter Vehicle VIN ") or "").strip(),
                    (row.get("service") or "").strip(),
                    classify_service_level(row.get("service")),
                    (row.get("Current Vehicle Milage") or "").strip(),
                    scheduled_date,
                    scheduled_date,
                    "Pending",
                    (row.get("service") or "").strip(),
                    public_notes,
                    source,
                    "No",
                    "No",
                    1,
                    now,
                    now,
                    legacy_source_key,
                ),
            )


def initialize_database():
    connection, backend = get_connection()
    try:
        _create_tables(connection, backend)
        _ensure_columns(connection, backend)
        _deduplicate_users(connection, backend)
        _ensure_unique_username_index(connection, backend)
        _ensure_indexes(connection, backend)
        _seed_plan_defaults(connection, backend)
        _migrate_legacy_users(connection, backend)
        _ensure_super_admin(connection, backend)
        _harden_default_credentials(connection, backend)
        _migrate_legacy_bookings(connection, backend)
        _import_csv_bookings(connection, backend)
        return {"backend": backend, "database_path": PRIMARY_SQLITE_PATH if backend == "sqlite" else "postgres"}
    finally:
        connection.close()


if __name__ == "__main__":
    state = initialize_database()
    print(f"Database ready: {state['backend']} ({state['database_path']})")
