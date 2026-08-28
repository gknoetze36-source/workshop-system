import logging
import os
import secrets
from datetime import datetime, timedelta

from .connection import require_postgres_for_service
from .query import _run, _db_bool
from .utils import utc_now
from app.core.domain.automation.catalog import WORKFLOW_DEFINITIONS

logger = logging.getLogger(__name__)

def _seed_plan_defaults(connection, backend):
    now = utc_now()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    default_subscription_end = (datetime.utcnow() + timedelta(days=30)).strftime("%Y-%m-%d")
    plans = {
        "basic": {"user_limit": 2, "automation_enabled": False, "chatbot_enabled": False, "reporting_enabled": False, "custom_integrations_enabled": False, "priority_support_enabled": False},
        "growth": {"user_limit": 10, "automation_enabled": True, "chatbot_enabled": True, "reporting_enabled": True, "custom_integrations_enabled": False, "priority_support_enabled": False},
        "premium": {"user_limit": 999999, "automation_enabled": True, "chatbot_enabled": True, "reporting_enabled": True, "custom_integrations_enabled": True, "priority_support_enabled": True},
    }
    locations = _run(connection, backend, "SELECT * FROM locations ORDER BY id") or []
    for location in locations:
        plan_code = (location.get("plan_code") or "basic").lower()
        plan = plans.get(plan_code, plans["basic"])
        _run(
            connection,
            backend,
            """
            UPDATE locations
            SET plan_code=%s,
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
                plan["user_limit"],
                _db_bool(plan["automation_enabled"], backend),
                _db_bool(plan["chatbot_enabled"], backend),
                _db_bool(plan["reporting_enabled"], backend),
                _db_bool(plan["custom_integrations_enabled"], backend),
                _db_bool(plan["priority_support_enabled"], backend),
                today,
                default_subscription_end,
                now,
                location["id"],
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
        (w.industry, w.name, w.event_type, w.trigger_timing, w.default_delay_minutes, w.default_message)
        for w in WORKFLOW_DEFINITIONS
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
            ) VALUES (%s, %s, %s, %s, %s, %s, 'whatsapp', %s, %s, %s)
            """,
            (industry, name, event_type, trigger_timing, delay_minutes, message, _db_bool(True, backend), now, now),
        )



def _ensure_super_admin(connection, backend):
    from werkzeug.security import generate_password_hash

    username = os.environ.get("SUPERADMIN_USERNAME", "admin@phanta.local").strip().lower()
    password = os.environ.get("SUPERADMIN_PASSWORD")
    if not password:
        if os.environ.get("FLASK_ENV", "").lower() == "production":
            raise RuntimeError("SUPERADMIN_PASSWORD is required in production")
        if os.environ.get("ALLOW_DEV_DEFAULT_CREDENTIALS", "").lower() not in {"1", "true", "yes"}:
            raise RuntimeError("SUPERADMIN_PASSWORD is required; set it explicitly for local development")
        password = os.environ.get("DEV_SUPERADMIN_PASSWORD")
        if not password:
            raise RuntimeError("DEV_SUPERADMIN_PASSWORD is required when ALLOW_DEV_DEFAULT_CREDENTIALS is enabled")
    full_name = os.environ.get("SUPERADMIN_NAME", "PHANTA Platform Super Admin")
    now = utc_now()

    existing = _run(
        connection, backend,
        "SELECT id FROM users WHERE role='super_admin' ORDER BY id LIMIT 1",
        one=True,
    )
    if existing:
        _run(
            connection, backend,
            """UPDATE users
               SET username=%s, email=%s, password=%s, password_hash=%s,
                   full_name=%s, active=%s, must_reset_password=%s, updated_at=%s
               WHERE id=%s""",
            (
                username, username, "", generate_password_hash(password),
                full_name, _db_bool(True, backend), _db_bool(False, backend),
                now, existing["id"],
            ),
        )
        return

    _run(
        connection, backend,
        """INSERT INTO users
           (username, email, password, password_hash, full_name, role, active,
            must_reset_password, created_at, updated_at)
           VALUES (%s,%s,%s,%s,%s,'super_admin',%s,%s,%s,%s)""",
        (
            username, username, "", generate_password_hash(password), full_name,
            _db_bool(True, backend), _db_bool(False, backend), now, now,
        ),
    )


def _upsert_bootstrap_user(connection, backend, username, password, role, full_name, location=None):
    from werkzeug.security import generate_password_hash

    now = utc_now()
    existing = _run(connection, backend, "SELECT id FROM users WHERE lower(username)=lower(%s)", (username,), one=True)
    # NOTE: this tuple previously carried the location id twice, a leftover
    # from the removed franchise/branch model where the second slot was a
    # separate scope column. That left two defects behind:
    #   - the UPDATE assigned location_id twice in one statement, which
    #     PostgreSQL rejects outright ("multiple assignments to same column")
    #   - the INSERT listed 12 columns against 13 placeholders
    # Both are fixed by carrying each value exactly once.
    user_values = (
        "",
        generate_password_hash(password),
        full_name,
        role,
        location["name"] if location else "",
        location["name"] if location else "",
        location["id"] if location else None,
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
            SET password=%s, password_hash=%s, full_name=%s, role=%s, location=%s, company=%s,
                location_id=%s, active=%s, must_reset_password=%s, updated_at=%s
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
            username, password, password_hash, full_name, role, location, company,
            location_id, active, must_reset_password, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (username, *user_values[:-1], now, now),
    )
