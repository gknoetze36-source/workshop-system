import csv
import logging
import threading
import os
import re
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus

BASE_DIR = Path(__file__).resolve().parent
PRIMARY_SQLITE_PATH = os.environ.get("SQLITE_PATH") or str(BASE_DIR / "database.db")
DEFAULT_FRANCHISE_NAME = os.environ.get("DEFAULT_FRANCHISE_NAME", "Main Workshop Group")
BOOKINGS_CSV_PATH = BASE_DIR / "bookings.csv"
logger = logging.getLogger(__name__)


def require_postgres_for_service():
    production_markers = (
        os.environ.get("REQUIRE_DATABASE_URL"),
        os.environ.get("RAILWAY_ENVIRONMENT"),
        os.environ.get("RAILWAY_SERVICE_ID"),
        os.environ.get("FLASK_ENV"),
        os.environ.get("APP_ENV"),
    )
    return any(str(value or "").lower() in {"1", "true", "yes", "production"} for value in production_markers)


_POOL = None
_POOL_LOCK = threading.Lock()
_LOCAL = threading.local()


class _PooledConnection:
    def __init__(self, pool, connection):
        self._pool = pool
        self._connection = connection
        self._closed = False

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def close(self):
        if not self._closed:
            self._pool.putconn(self._connection)
            self._closed = True


def _postgres_pool():
    global _POOL
    if _POOL is None:
        with _POOL_LOCK:
            if _POOL is None:
                from psycopg2.pool import ThreadedConnectionPool

                database_url = os.environ.get("DATABASE_URL")
                minconn = int(os.environ.get("PGPOOL_MINCONN", "1"))
                maxconn = int(os.environ.get("PGPOOL_MAXCONN", "5"))
                timeout = int(os.environ.get("PGCONNECT_TIMEOUT", "5"))
                _POOL = ThreadedConnectionPool(minconn, maxconn, database_url, connect_timeout=timeout)
    return _POOL


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
        connection = _PooledConnection(_postgres_pool(), _postgres_pool().getconn())
        connection.autocommit = False
        return connection, "postgres"

    if require_postgres_for_service():
        raise RuntimeError("DATABASE_URL is required for this Railway service. Add DATABASE_URL=${{Postgres.DATABASE_URL}} to the service variables.")

    connection = sqlite3.connect(PRIMARY_SQLITE_PATH)
    connection.row_factory = sqlite3.Row
    return connection, "sqlite"


def _adapt_query(query, backend):
    if backend == "sqlite":
        return query.replace("%s", "?")
    return query


def _db_bool(value, backend):
    return bool(value) if backend == "postgres" else int(bool(value))


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
        if not getattr(_LOCAL, "in_transaction", False):
            connection.commit()
        return None
    finally:
        cursor.close()


def query_db(query, args=(), one=False):
    active = getattr(_LOCAL, "connection", None)
    if active:
        connection, backend = active
        return _run(connection, backend, query, args, one=one)

    connection, backend = get_connection()
    try:
        result = _run(connection, backend, query, args, one=one)
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def execute_db(query, args=()):
    query_db(query, args=args, one=False)


@contextmanager
def transaction():
    if getattr(_LOCAL, "connection", None):
        yield
        return

    connection, backend = get_connection()
    _LOCAL.connection = (connection, backend)
    _LOCAL.in_transaction = True
    try:
        yield
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        _LOCAL.connection = None
        _LOCAL.in_transaction = False
        connection.close()


def run_alembic_migrations():
    if os.environ.get("SKIP_ALEMBIC_MIGRATIONS", "").lower() in {"1", "true", "yes"}:
        return
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return
    from alembic import command
    from alembic.config import Config

    config = Config(str(BASE_DIR / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", database_url)
    try:
        command.upgrade(config, "head")
    except Exception:
        logger.exception("alembic_migration_failed")
        if os.environ.get("STRICT_ALEMBIC_MIGRATIONS", "").lower() in {"1", "true", "yes"}:
            raise


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
            industry TEXT DEFAULT 'workshop',
            subscription_status TEXT DEFAULT 'active',
            subscription_start TEXT,
            subscription_end TEXT,
            setup_fee REAL DEFAULT 0,
            public_base_url TEXT,
            inbound_webhook_token TEXT,
            plan_code TEXT DEFAULT 'basic',
            branch_limit INTEGER DEFAULT 1,
            user_limit INTEGER DEFAULT 2,
            automation_enabled {integer_boolean} DEFAULT FALSE,
            chatbot_enabled {integer_boolean} DEFAULT FALSE,
            reporting_enabled {integer_boolean} DEFAULT FALSE,
            custom_integrations_enabled {integer_boolean} DEFAULT FALSE,
            priority_support_enabled {integer_boolean} DEFAULT FALSE,
            monthly_base_price REAL DEFAULT 0,
            monthly_message_limit INTEGER DEFAULT 2000,
            messages_used INTEGER DEFAULT 0,
            overage_price_per_message REAL DEFAULT 0.5,
            billing_day TEXT,
            active {integer_boolean} DEFAULT TRUE,
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
            public_booking_enabled {integer_boolean} DEFAULT TRUE,
            active {integer_boolean} DEFAULT TRUE,
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
            active {integer_boolean} DEFAULT TRUE,
            must_reset_password {integer_boolean} DEFAULT FALSE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS customers (
            id {primary_key},
            franchise_id INTEGER,
            first_name TEXT,
            surname TEXT,
            full_name TEXT,
            phone TEXT,
            email TEXT,
            accepts_whatsapp {integer_boolean} DEFAULT TRUE,
            accepts_sms {integer_boolean} DEFAULT TRUE,
            metadata_json TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS services (
            id {primary_key},
            franchise_id INTEGER,
            branch_id INTEGER,
            name TEXT NOT NULL,
            category TEXT,
            duration_minutes INTEGER DEFAULT 60,
            price_amount REAL DEFAULT 0,
            active {integer_boolean} DEFAULT TRUE,
            metadata_json TEXT,
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
            customer_id INTEGER,
            service_id INTEGER,
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
            whatsapp_opt_in {integer_boolean} DEFAULT FALSE,
            privacy_consent_at TEXT,
            reminder_opt_in {integer_boolean} DEFAULT TRUE,
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
            active {integer_boolean} DEFAULT TRUE,
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
            processed {integer_boolean} DEFAULT FALSE,
            privacy_notice_sent {integer_boolean} DEFAULT FALSE,
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
            declined {integer_boolean} DEFAULT FALSE,
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
        f"""
        CREATE TABLE IF NOT EXISTS industry_templates (
            id {primary_key},
            industry TEXT NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            default_plan TEXT DEFAULT 'basic',
            default_message_limit INTEGER DEFAULT 2000,
            active {integer_boolean} DEFAULT TRUE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS automation_templates (
            id {primary_key},
            industry TEXT NOT NULL,
            name TEXT NOT NULL,
            event_type TEXT NOT NULL,
            trigger_timing TEXT,
            default_delay_minutes INTEGER DEFAULT 0,
            default_message TEXT,
            channel_priority TEXT DEFAULT 'whatsapp,sms',
            active {integer_boolean} DEFAULT TRUE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS automation_rules (
            id {primary_key},
            franchise_id INTEGER,
            template_id INTEGER,
            name TEXT NOT NULL,
            event_type TEXT NOT NULL,
            conditions_json TEXT,
            action_json TEXT,
            delay_minutes INTEGER DEFAULT 0,
            active {integer_boolean} DEFAULT TRUE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS scheduled_jobs (
            id {primary_key},
            franchise_id INTEGER,
            automation_rule_id INTEGER,
            job_type TEXT NOT NULL,
            payload_json TEXT,
            scheduled_for TEXT,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            max_attempts INTEGER DEFAULT 3,
            locked_at TEXT,
            completed_at TEXT,
            last_error TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS automation_logs (
            id {primary_key},
            franchise_id INTEGER,
            automation_rule_id INTEGER,
            scheduled_job_id INTEGER,
            event_type TEXT,
            status TEXT,
            message TEXT,
            created_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS failed_jobs (
            id {primary_key},
            franchise_id INTEGER,
            scheduled_job_id INTEGER,
            error_message TEXT,
            payload_json TEXT,
            failed_at TEXT,
            resolved {integer_boolean} DEFAULT FALSE,
            resolved_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS billing_records (
            id {primary_key},
            franchise_id INTEGER,
            amount REAL DEFAULT 0,
            base_amount REAL DEFAULT 0,
            usage_amount REAL DEFAULT 0,
            status TEXT DEFAULT 'unpaid',
            billing_period TEXT,
            payment_reference_id TEXT,
            payment_link TEXT,
            paid_at TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS usage_daily (
            id {primary_key},
            franchise_id INTEGER,
            usage_date TEXT,
            messages_used INTEGER DEFAULT 0,
            extra_messages INTEGER DEFAULT 0,
            extra_cost REAL DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS onboarding_sessions (
            id {primary_key},
            franchise_id INTEGER,
            industry TEXT,
            selected_plan TEXT,
            status TEXT DEFAULT 'started',
            current_step TEXT,
            started_at TEXT,
            completed_at TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS onboarding_answers (
            id {primary_key},
            franchise_id INTEGER,
            session_id INTEGER,
            question_key TEXT,
            answer_value TEXT,
            created_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS onboarding_state (
            id {primary_key},
            franchise_id INTEGER,
            setup_progress INTEGER DEFAULT 0,
            payment_completed {integer_boolean} DEFAULT FALSE,
            whatsapp_connected {integer_boolean} DEFAULT FALSE,
            services_created {integer_boolean} DEFAULT FALSE,
            automations_enabled {integer_boolean} DEFAULT FALSE,
            go_live_ready {integer_boolean} DEFAULT FALSE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS feature_flags (
            id {primary_key},
            franchise_id INTEGER,
            feature_key TEXT NOT NULL,
            enabled {integer_boolean} DEFAULT FALSE,
            created_at TEXT,
            updated_at TEXT
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
            "industry": "TEXT DEFAULT 'workshop'",
            "subscription_status": "TEXT DEFAULT 'active'",
            "subscription_start": "TEXT",
            "subscription_end": "TEXT",
            "setup_fee": "REAL DEFAULT 0",
            "public_base_url": "TEXT",
            "inbound_webhook_token": "TEXT",
            "plan_code": "TEXT DEFAULT 'basic'",
            "branch_limit": "INTEGER DEFAULT 1",
            "user_limit": "INTEGER DEFAULT 2",
            "automation_enabled": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "chatbot_enabled": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "reporting_enabled": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "custom_integrations_enabled": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "priority_support_enabled": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "monthly_base_price": "REAL DEFAULT 0",
            "monthly_message_limit": "INTEGER DEFAULT 2000",
            "messages_used": "INTEGER DEFAULT 0",
            "overage_price_per_message": "REAL DEFAULT 0.5",
            "billing_day": "TEXT",
            "active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
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
            "public_booking_enabled": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
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
            "active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "must_reset_password": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "bookings": {
            "booking_reference": "TEXT",
            "franchise_id": "INTEGER",
            "branch_id": "INTEGER",
            "customer_id": "INTEGER",
            "service_id": "INTEGER",
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
            "reminder_opt_in": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
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
            "whatsapp_opt_in": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
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
            "active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
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
            "processed": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "privacy_notice_sent": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "customers": {
            "franchise_id": "INTEGER",
            "first_name": "TEXT",
            "surname": "TEXT",
            "full_name": "TEXT",
            "phone": "TEXT",
            "email": "TEXT",
            "accepts_whatsapp": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "accepts_sms": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "metadata_json": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "services": {
            "franchise_id": "INTEGER",
            "branch_id": "INTEGER",
            "name": "TEXT",
            "category": "TEXT",
            "duration_minutes": "INTEGER DEFAULT 60",
            "price_amount": "REAL DEFAULT 0",
            "active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "metadata_json": "TEXT",
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
            "declined": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
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
        "industry_templates": {
            "industry": "TEXT",
            "name": "TEXT",
            "description": "TEXT",
            "default_plan": "TEXT DEFAULT 'basic'",
            "default_message_limit": "INTEGER DEFAULT 2000",
            "active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "automation_templates": {
            "industry": "TEXT",
            "name": "TEXT",
            "event_type": "TEXT",
            "trigger_timing": "TEXT",
            "default_delay_minutes": "INTEGER DEFAULT 0",
            "default_message": "TEXT",
            "channel_priority": "TEXT DEFAULT 'whatsapp,sms'",
            "active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "automation_rules": {
            "franchise_id": "INTEGER",
            "template_id": "INTEGER",
            "name": "TEXT",
            "event_type": "TEXT",
            "conditions_json": "TEXT",
            "action_json": "TEXT",
            "delay_minutes": "INTEGER DEFAULT 0",
            "active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "scheduled_jobs": {
            "franchise_id": "INTEGER",
            "automation_rule_id": "INTEGER",
            "job_type": "TEXT",
            "payload_json": "TEXT",
            "scheduled_for": "TEXT",
            "status": "TEXT DEFAULT 'pending'",
            "attempts": "INTEGER DEFAULT 0",
            "max_attempts": "INTEGER DEFAULT 3",
            "locked_at": "TEXT",
            "completed_at": "TEXT",
            "last_error": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "automation_logs": {
            "franchise_id": "INTEGER",
            "automation_rule_id": "INTEGER",
            "scheduled_job_id": "INTEGER",
            "event_type": "TEXT",
            "status": "TEXT",
            "message": "TEXT",
            "created_at": "TEXT",
        },
        "failed_jobs": {
            "franchise_id": "INTEGER",
            "scheduled_job_id": "INTEGER",
            "error_message": "TEXT",
            "payload_json": "TEXT",
            "failed_at": "TEXT",
            "resolved": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "resolved_at": "TEXT",
        },
        "billing_records": {
            "franchise_id": "INTEGER",
            "amount": "REAL DEFAULT 0",
            "base_amount": "REAL DEFAULT 0",
            "usage_amount": "REAL DEFAULT 0",
            "status": "TEXT DEFAULT 'unpaid'",
            "billing_period": "TEXT",
            "payment_reference_id": "TEXT",
            "payment_link": "TEXT",
            "paid_at": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "usage_daily": {
            "franchise_id": "INTEGER",
            "usage_date": "TEXT",
            "messages_used": "INTEGER DEFAULT 0",
            "extra_messages": "INTEGER DEFAULT 0",
            "extra_cost": "REAL DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "onboarding_sessions": {
            "franchise_id": "INTEGER",
            "industry": "TEXT",
            "selected_plan": "TEXT",
            "status": "TEXT DEFAULT 'started'",
            "current_step": "TEXT",
            "started_at": "TEXT",
            "completed_at": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "onboarding_answers": {
            "franchise_id": "INTEGER",
            "session_id": "INTEGER",
            "question_key": "TEXT",
            "answer_value": "TEXT",
            "created_at": "TEXT",
        },
        "onboarding_state": {
            "franchise_id": "INTEGER",
            "setup_progress": "INTEGER DEFAULT 0",
            "payment_completed": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "whatsapp_connected": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "services_created": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "automations_enabled": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "go_live_ready": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "feature_flags": {
            "franchise_id": "INTEGER",
            "feature_key": "TEXT",
            "enabled": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
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
        "CREATE INDEX IF NOT EXISTS idx_bookings_customer ON bookings(franchise_id, customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_customers_scope ON customers(franchise_id, phone, email)",
        "CREATE INDEX IF NOT EXISTS idx_services_scope ON services(franchise_id, branch_id, name)",
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
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_industry_templates_industry ON industry_templates(industry)",
        "CREATE INDEX IF NOT EXISTS idx_automation_templates_industry ON automation_templates(industry, event_type)",
        "CREATE INDEX IF NOT EXISTS idx_automation_rules_scope ON automation_rules(franchise_id, event_type, active)",
        "CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_due ON scheduled_jobs(status, scheduled_for)",
        "CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_scope ON scheduled_jobs(franchise_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_automation_logs_scope ON automation_logs(franchise_id, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_failed_jobs_scope ON failed_jobs(franchise_id, resolved, failed_at)",
        "CREATE INDEX IF NOT EXISTS idx_billing_records_scope ON billing_records(franchise_id, billing_period, status)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_usage_daily_scope ON usage_daily(franchise_id, usage_date)",
        "CREATE INDEX IF NOT EXISTS idx_onboarding_sessions_scope ON onboarding_sessions(franchise_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_onboarding_answers_session ON onboarding_answers(session_id, question_key)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_onboarding_state_franchise ON onboarding_state(franchise_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_feature_flags_scope ON feature_flags(franchise_id, feature_key)",
    ]
    for query in index_queries:
        _run(connection, backend, query)


def _seed_plan_defaults(connection, backend):
    now = utc_now()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    default_subscription_end = (datetime.utcnow() + timedelta(days=30)).strftime("%Y-%m-%d")
    plans = {
        "basic": {"branch_limit": 1, "user_limit": 2, "automation_enabled": False, "chatbot_enabled": False, "reporting_enabled": False, "custom_integrations_enabled": False, "priority_support_enabled": False},
        "growth": {"branch_limit": 5, "user_limit": 10, "automation_enabled": True, "chatbot_enabled": True, "reporting_enabled": True, "custom_integrations_enabled": False, "priority_support_enabled": False},
        "premium": {"branch_limit": 999999, "user_limit": 999999, "automation_enabled": True, "chatbot_enabled": True, "reporting_enabled": True, "custom_integrations_enabled": True, "priority_support_enabled": True},
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
                industry=COALESCE(NULLIF(industry, ''), 'workshop'),
                subscription_status=COALESCE(NULLIF(subscription_status, ''), 'active'),
                subscription_start=COALESCE(NULLIF(subscription_start, ''), %s),
                subscription_end=COALESCE(NULLIF(subscription_end, ''), %s),
                setup_fee=COALESCE(setup_fee, 0),
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
                _db_bool(plan["automation_enabled"], backend),
                _db_bool(plan["chatbot_enabled"], backend),
                _db_bool(plan["reporting_enabled"], backend),
                _db_bool(plan["custom_integrations_enabled"], backend),
                _db_bool(plan["priority_support_enabled"], backend),
                today,
                default_subscription_end,
                now,
                franchise["id"],
            ),
        )


def _seed_saas_templates(connection, backend):
    now = utc_now()
    industries = [
        ("workshop", "Workshop", "Vehicle service, repair, maintenance, yearly service reminders, and missed booking recovery.", "growth", 5000),
        ("salon", "Salon", "Appointments, confirmations, simple reminders, rebooking prompts, and no-show recovery.", "basic", 2000),
        ("dentist", "Dentist", "Appointment reminders, missed appointment recovery, treatment follow-ups, and six-month checkups.", "growth", 4000),
        ("clinic", "Clinic", "Appointment reminders, follow-ups, recurring care reminders, and patient communication workflows.", "growth", 5000),
        ("hotel", "Hotel", "Booking confirmations, check-in reminders, check-out messages, and review requests.", "growth", 6000),
        ("consultant", "Consultant", "Consultation bookings, confirmations, follow-ups, and lead recovery.", "basic", 2000),
        ("gym", "Gym", "Membership follow-ups, class bookings, attendance recovery, and renewal reminders.", "growth", 5000),
        ("cleaning", "Cleaning Company", "Job bookings, staff dispatch reminders, recurring cleaning reminders, and follow-ups.", "growth", 4000),
        ("repair", "Repair Business", "Repair bookings, quote follow-ups, status updates, and collection reminders.", "growth", 4000),
    ]
    for industry, name, description, default_plan, message_limit in industries:
        existing = _run(connection, backend, "SELECT id FROM industry_templates WHERE industry=%s", (industry,), one=True)
        if existing:
            continue
        _run(
            connection,
            backend,
            """
            INSERT INTO industry_templates (industry, name, description, default_plan, default_message_limit, active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (industry, name, description, default_plan, message_limit, _db_bool(True, backend), now, now),
        )

    automation_templates = [
        ("workshop", "Booking confirmation", "booking.created", "immediate", 0, "Your booking is confirmed. We will see you at {business_name}."),
        ("workshop", "Missed booking recovery", "booking.missed", "same_day", 60, "We missed you today. Would you like us to help book a new time?"),
        ("workshop", "Yearly service reminder", "service.annual_due", "annual", 0, "Your yearly service reminder is due. Would you like us to book you in?"),
        ("salon", "Booking confirmation", "booking.created", "immediate", 0, "Your appointment is confirmed with {business_name}."),
        ("salon", "No-show recovery", "booking.missed", "same_day", 30, "We missed you today. Would you like another appointment?"),
        ("dentist", "Appointment reminder", "booking.reminder_due", "day_before", 1440, "Reminder: your dental appointment is coming up with {business_name}."),
        ("dentist", "Checkup reminder", "service.recurring_due", "six_monthly", 0, "It is time for your dental checkup. Would you like to book?"),
        ("clinic", "Appointment reminder", "booking.reminder_due", "day_before", 1440, "Reminder: your appointment is coming up with {business_name}."),
        ("hotel", "Check-in reminder", "booking.reminder_due", "day_before", 1440, "Your check-in at {business_name} is coming up. Reply if you need help."),
        ("consultant", "Lead follow-up", "inquiry.created", "after_delay", 30, "Thanks for reaching out. Would you like me to secure a consultation time?"),
        ("gym", "Class reminder", "booking.reminder_due", "same_day", 120, "Reminder: your class/session at {business_name} is coming up."),
        ("cleaning", "Job confirmation", "booking.created", "immediate", 0, "Your cleaning booking is confirmed with {business_name}."),
        ("repair", "Quote follow-up", "quote.pending", "after_delay", 120, "Just following up on your quote. Would you like us to proceed?"),
    ]
    for industry, name, event_type, trigger_timing, delay_minutes, message in automation_templates:
        existing = _run(
            connection,
            backend,
            "SELECT id FROM automation_templates WHERE industry=%s AND name=%s AND event_type=%s",
            (industry, name, event_type),
            one=True,
        )
        if existing:
            continue
        _run(
            connection,
            backend,
            """
            INSERT INTO automation_templates (
                industry, name, event_type, trigger_timing, default_delay_minutes, default_message,
                channel_priority, active, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, 'whatsapp,sms', %s, %s, %s)
            """,
            (industry, name, event_type, trigger_timing, delay_minutes, message, _db_bool(True, backend), now, now),
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
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (name, slug, contact_email, contact_phone, _db_bool(True, backend), now, now),
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (franchise_id, name, slug, contact_email, contact_phone, location, _db_bool(True, backend), _db_bool(True, backend), now, now),
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
    password = os.environ.get("SUPERADMIN_PASSWORD")
    full_name = os.environ.get("SUPERADMIN_NAME", "Platform Super Admin")
    if not password:
        if require_postgres_for_service():
            password = secrets.token_urlsafe(32)
            logger.warning("SUPERADMIN_PASSWORD is not set; created super admin with a generated password. Set SUPERADMIN_PASSWORD and reset this account before use.")
        else:
            password = "ChangeMeNow!2026"
    now = utc_now()

    _run(
        connection,
        backend,
        """
        INSERT INTO users (
            username, password, password_hash, full_name, role, active,
            must_reset_password, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, 'super_admin', %s, %s, %s, %s)
        """,
        (username, "", generate_password_hash(password), full_name, _db_bool(True, backend), _db_bool(True, backend), now, now),
    )


def _upsert_bootstrap_user(connection, backend, username, password, role, full_name, franchise=None, branch=None):
    from werkzeug.security import generate_password_hash

    now = utc_now()
    existing = _run(connection, backend, "SELECT id FROM users WHERE lower(username)=lower(%s)", (username,), one=True)
    user_values = (
        "",
        generate_password_hash(password),
        full_name,
        role,
        branch["name"] if branch else "",
        franchise["name"] if franchise else "",
        franchise["id"] if franchise else None,
        branch["id"] if branch else None,
        _db_bool(True, backend),
        _db_bool(False, backend),
        now,
    )
    if existing:
        _run(
            connection,
            backend,
            """
            UPDATE users
            SET password=%s, password_hash=%s, full_name=%s, role=%s, branch=%s, company=%s,
                franchise_id=%s, branch_id=%s, active=%s, must_reset_password=%s, updated_at=%s
            WHERE id=%s
            """,
            (*user_values, existing["id"]),
        )
        return

    _run(
        connection,
        backend,
        """
        INSERT INTO users (
            username, password, password_hash, full_name, role, branch, company,
            franchise_id, branch_id, active, must_reset_password, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (username, *user_values[:-1], now, now),
    )


def _ensure_demo_access_accounts(connection, backend):
    superadmin_username = os.environ.get("DEMO_SUPERADMIN_USERNAME", "superadmin")
    superadmin_password = os.environ.get("DEMO_SUPERADMIN_PASSWORD", "SuperAdmin2026!")
    reception_username = os.environ.get("DEMO_RECEPTION_USERNAME", "demo.reception")
    reception_password = os.environ.get("DEMO_RECEPTION_PASSWORD", "DemoReception2026!")

    franchise = _get_or_create_franchise(connection, backend, "Demo Motor Group", "demo@example.com", "+27000000000")
    branch = _get_or_create_branch(
        connection,
        backend,
        franchise["id"],
        "Demo Reception Branch",
        "reception@example.com",
        "+27000000001",
        "Demo City",
    )

    now = utc_now()
    _run(
        connection,
        backend,
        """
        UPDATE franchises
        SET plan_code='premium',
            branch_limit=999999,
            user_limit=999999,
            automation_enabled=%s,
            chatbot_enabled=%s,
            reporting_enabled=%s,
            custom_integrations_enabled=%s,
            priority_support_enabled=%s,
            monthly_message_limit=999999,
            subscription_status='active',
            updated_at=%s
        WHERE id=%s
        """,
        (
            _db_bool(True, backend),
            _db_bool(True, backend),
            _db_bool(True, backend),
            _db_bool(True, backend),
            _db_bool(True, backend),
            now,
            franchise["id"],
        ),
    )
    franchise = _run(connection, backend, "SELECT * FROM franchises WHERE id=%s", (franchise["id"],), one=True)

    _upsert_bootstrap_user(connection, backend, superadmin_username, superadmin_password, "super_admin", "Platform Super Admin")
    _upsert_bootstrap_user(connection, backend, reception_username, reception_password, "reception", "Demo Reception", franchise, branch)


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
        if backend == "postgres":
            run_alembic_migrations()
        _ensure_unique_username_index(connection, backend)
        _ensure_indexes(connection, backend)
        _seed_plan_defaults(connection, backend)
        _seed_saas_templates(connection, backend)
        _ensure_super_admin(connection, backend)
        run_legacy_bootstrap = backend == "sqlite" or os.environ.get("RUN_LEGACY_BOOTSTRAP", "").lower() in {"1", "true", "yes"}
        if run_legacy_bootstrap:
            _deduplicate_users(connection, backend)
            _migrate_legacy_users(connection, backend)
            _harden_default_credentials(connection, backend)
            _migrate_legacy_bookings(connection, backend)
            _import_csv_bookings(connection, backend)
        _ensure_demo_access_accounts(connection, backend)
        return {"backend": backend, "database_path": PRIMARY_SQLITE_PATH if backend == "sqlite" else "postgres"}
    finally:
        connection.close()


if __name__ == "__main__":
    state = initialize_database()
    print(f"Database ready: {state['backend']} ({state['database_path']})")
