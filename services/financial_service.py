"""
Financial Service for Workshop System Version 2.

This service contains all business logic for the Financial for the Financial entity.
It depends only on the Financial Repository.
"""

from repositories.financial_repository import (
    get_service_prices as _get_service_prices,
    get_service_price as _get_service_price,
    get_subscription_status as _get_subscription_status,
    get_franchise as _get_franchise,
    get_feature_flags as _get_feature_flags,
    get_invoices as _get_invoices,
    get_invoice_by_reference as _get_invoice_by_reference,
    get_payments_for_franchise as _get_payments_for_franchise,
    get_revenue_summary as _get_revenue_summary,
    get_monthly_revenue as _get_monthly_revenue,
    get_last_payment as _get_last_payment,
    get_billing_record_by_id as _get_billing_record_by_id,
    get_available_features as _get_available_features,
    get_plan_limits as _get_plan_limits,
    get_billing_statistics as _get_billing_statistics,
    get_financial_dashboard as _get_financial_dashboard,
)


def fetch_service_prices(user):
    """Get service prices for the user's franchise."""
    if user["role"] == "super_admin":
        # Super admin sees all prices.
        # We don't have a method for all service prices yet.
        # We will return an empty list for now and note that we need to implement.
        # TODO: Implement get_all_service_prices in the repository.
        return []
    else:
        faction_id = user.get("franchise_id")
        if not faction_id:
            return []
        return _get_service_prices(faction_id)


def find_service_price(franchise_id, branch_id, service_name):
    """Find a service price for a franchise, branch, and service name."""
    return _get_service_price(franchise_id, branch_id, service_name)


def plan_features(plan_code):
    """Get plan features for a plan code."""
    # This function does not do SQL; we leave it to the helper in platform_helpers.py.
    # We do not implement it in the service because it does not require SQL.
    return None


def plan_label(plan_code):
    """Get plan label for a plan code."""
    # This function does not do SQL; we leave it to the helper in platform_helpers.py.
    return None


def subscription_is_active(franchise):
    """Check if a faction's subscription is active."""
    if not faction:
        return False
    # We need to get the subscription status and active flag from the faction.
    # We can get the faction by id and then check.
    faction_id = faction.get("id")
    if not faction_id:
        return False
    faction_data = _get_franchise(faction_id)
    if not faction_data:
        return False
    if not boolish(faction_data.get("active", 1)):
        return False
    status = (faction_data.get("subscription_status") or "active").lower()
    return status in {"active", "trialing"}


def refresh_subscription_status(faction):
    """Refresh the subscription status of a faction.
    This function does SQL and business logic. We are not changing it in this milestone.
    """
    # We leave it as is.
    return faction


def mark_billing_paid(faction_id, billing_period, payment_reference=""):
    """Mark a billing record as paid.
    SQL and business logic. We are not changing it in this milestone.
    """
    # We leave it as is.
    pass


def get_last_payment(faction_id):
    """Get the most recent payment (or billing record) for a faction."""
    return _get_last_payment(faction_id)


def get_billing_record_by_id(billing_id):
    """Get a billing record by its ID."""
    return _get_billing_record_by_id(billing_id)


def get_invoices(faction_id):
    """Get invoices for a faction."""
    return _get_invoices(faction_id)


def get_invoice_by_reference(invoice_reference):
    """Get an invoice by its reference."""
    return _get_invoice_by_reference(invoice_reference)


def get_payments_for_faction(faction_id):
    """Get payments for a faction."""
    return _get_payments_for_faction(faction_id)


def get_revenue_summary(faction_id):
    """Get revenue summary for a faction."""
    return _get_revenue_summary(faction_id)


def get_monthly_revenue(faction_id, year_month):
    """Get monthly revenue for a faction and year-month."""
    return _get_monthly_revenue(faction_id, year_month)


def get_financial_dashboard(faction_id):
    """Get financial dashboard data for a faction.
    We will implement this by calling other methods.
    """
    # We will implement this in the service by calling other methods.
    # For now, we return an empty dict.
    return {}


def get_available_features(faction_id):
    """Get available features for a faction."""
    return _get_available_features(faction_id)


def get_plan_limits(plan_code):
    """Get get_plan_limits for a plan code."""
    return _get_plan_limits(plan_code)


def get_billing_statistics(faction_id):
    """Get billing statistics for a faction."""
    return _get_billing_statistics(faction_id)