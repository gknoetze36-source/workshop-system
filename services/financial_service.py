"""
Financial Service for Workshop System Version 2.

This service contains financial business logic and delegates database access
to the Financial Repository.
"""
from database import execute_db, query_db, utc_now, fetch_one, fetch_all
from helpers.dates import parse_date, utc_today
from datetime import datetime, timedelta
import os


from helpers.common import boolish
from database import transaction
from services.usage_service import track_message_usage
from services.billing_service import (
    close_billing_period,
    mark_billing_paid,
    create_payment_link,
    expire_due_subscriptions,
)
from services.usage_reporting_service import monthly_usage_summary, daily_usage_summary


from repositories.financial_repository import (
    get_billing_record_by_id as _get_billing_record_by_id,
    get_invoice_by_reference as _get_invoice_by_reference,
    get_invoices as _get_invoices,
    get_last_payment as _get_last_payment,
    get_monthly_revenue as _get_monthly_revenue,
    get_payments_for_location as _get_payments_for_location,
    get_revenue_summary as _get_revenue_summary,
)


def get_last_payment(location_id):
    """Get the most recent billing payment for a location."""
    return _get_last_payment(location_id)

def plan_features(location):
    """Return the features included in the Core subscription."""
    return [
        "Core Platform",
        "Unlimited locations",
        "Unlimited users",
        "Automations included",
        "AI Chatbot included",
        "Reporting included",
        "Priority support included",
        "Custom integrations included",
    ]


def get_billing_record_by_id(billing_id):
    """Get a billing record by ID."""
    return _get_billing_record_by_id(billing_id)


def get_invoices(location_id):
    """Get invoices for a location."""
    return _get_invoices(location_id)


def get_invoice_by_reference(invoice_reference):
    """Get an invoice by reference."""
    return _get_invoice_by_reference(invoice_reference)


def get_payments_for_location(location_id):
    """Get payments for a location."""
    return _get_payments_for_location(location_id)


def get_revenue_summary(location_id):
    """Get the revenue summary for a location."""
    return _get_revenue_summary(location_id)


def get_monthly_revenue(location_id, year_month):
    """Get monthly revenue for a location and billing month."""
    return _get_monthly_revenue(location_id, year_month)


def refresh_subscription_status(location):
    if not location:
        return None
    subscription_end = parse_date(location.get("subscription_end"))
    """Update subscription status if the subscription has expired."""
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

    

    
    

    
    
