"""Financial Service -- subscription/feature-gating.

Repaired: this file previously also re-exported repositories.financial_repository
(get_last_payment, plan_features, get_billing_record_by_id, get_invoices,
get_invoice_by_reference, get_payments_for_location, get_revenue_summary,
get_monthly_revenue) -- confirmed, this loop, to have zero callers anywhere in
the repository, for every one of those functions, and repositories/financial_repository.py
itself had zero callers for ANY of its functions, from any file. Deleted, not
repaired: the real billing source of truth (VANTA subscription -> billing_records
-> Paystack -> webhook -> reconciliation -> reporting) is services/billing_service.py,
confirmed by its own real caller (integrations/paystack/webhooks/event_handlers/
charge_handlers.py) -- these functions were a separate, unwired, duplicated
attempt at billing data access, never connected to that live workflow at all.

What remains below -- refresh_subscription_status() through can_send_messages()
-- is the genuinely live half: can_create_booking() gates
services/booking_service.py's own booking creation, can_send_messages() gates
services/messaging_service.py's own send path. Confirmed by direct caller
search, not assumed.
"""
from datetime import datetime

from database import execute_db, utc_now, fetch_one
from helpers.dates import parse_date
from helpers.common import boolish
from services.usage_service import track_message_usage  # noqa: F401 -- re-exported; services/reminder_service.py imports this name from here, not from usage_service directly


def refresh_subscription_status(location):
    """Update subscription status if the subscription has expired.

    The docstring previously sat two statements down, which made it a no-op
    string expression rather than the function's documentation.
    """
    if not location:
        return None
    subscription_end = parse_date(location.get("subscription_end"))
    current_status = (location.get("subscription_status") or "active").lower()
    if subscription_end and subscription_end.date() < datetime.utcnow().date() and current_status not in {"inactive", "cancelled"}:
        execute_db(
            "UPDATE locations SET subscription_status='inactive', updated_at=%s WHERE id=%s",
            (utc_now(), location["id"]),
        )
        location = dict(location)
        location["subscription_status"] = "inactive"
    return location


def subscription_status(location):
    location = refresh_subscription_status(location)
    if not location:
        return "inactive"
    if not boolish(location.get("active", 1)):
        return "inactive"
    return (location.get("subscription_status") or "active").lower()


def subscription_is_active(location):
    return subscription_status(location) in {"active", "trialing"}


def feature_enabled(location, feature_key):
    if not location:
        return False
    flag = fetch_one(
        "SELECT enabled FROM feature_flags WHERE location_id=%s AND feature_key=%s",
        (location["id"], feature_key),
    )
    if flag is not None:
        return boolish(flag.get("enabled", 0))
    return False

def can_use_paid_feature(location, feature_key=None):
    """Return True if the location can use a paid feature."""
    if not subscription_is_active(location):
        return False
    return feature_enabled(location, feature_key) if feature_key else True


def can_create_booking(location):
    """Return True if bookings are allowed."""
    return can_use_paid_feature(location)


def can_run_automation(location):
    """Return True if automations are allowed."""
    return can_use_paid_feature(location, "automation_enabled")


def can_send_messages(location):
    """Return True if messaging is allowed."""
    return can_use_paid_feature(location)
