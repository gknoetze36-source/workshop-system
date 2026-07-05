"""
Financial Repository for Workshop System Version 2.

This repository handles all database operations for the Financial entity.
It interacts with the existing financial tables: billing_records, invoices, service_prices, franchises, feature_flags, etc.
"""

from database import query_db


def get_service_prices(franchise_id):
    """Get all service prices for a franchise with franchise and branch names."""
    sql = """
        SELECT sp.*, f.name AS franchise_name, b.name AS branch_name
        FROM service_prices sp
        LEFT JOIN franchises f ON f.id = sp.franchise_id
        LEFT JOIN branches b ON b.id = sp.branch_id
        WHERE sp.franchise_id = %s
        ORDER BY f.name, b.name, sp.service_name
    """
    return query_db(sql, (franchise_id,))


def get_service_price(franchise_id, branch_id, service_name):
    """Get a service price for a franchise, branch, and service name."""
    service_name = (service_name or "").strip()
    if not service_name:
        return None
    # Try with branch_id first
    price = query_db(
        """
        SELECT * FROM service_prices
        WHERE franchise_id=%s AND branch_id=%s AND lower(service_name)=lower(%s) AND COALESCE(active,TRUE)=TRUE
        """,
        (franchise_id, branch_id, service_name),
        one=True,
    )
    if price:
        return price
    # Then try without branch_id (i.e., branch_id IS NULL)
    return query_db(
        """
        SELECT * FROM service_prices
        WHERE franchise_id=%s AND branch_id IS NULL AND lower(service_name)=lower(%s) AND COALESCE(active,TRUE)=TRUE
        """,
        (franchise_id, service_name),
        one=True,
    )


def get_subscription_status(franchise_id):
    """Get the subscription status for a franchise."""
    sql = """
        SELECT subscription_status, active
        FROM franchises
        WHERE id = %s
    """
    return query_db(sql, (franchise_id,), one=True)


def get_franchise(franchise_id):
    """Get a franchise by ID."""
    sql = "SELECT * FROM franchises WHERE id = %s"
    return query_db(sql, (franchise_id,), one=True)


def get_feature_flags(franchise_id):
    """Get feature flags for a franchise."""
    sql = """
        SELECT feature_key, enabled
        FROM feature_flags
        WHERE franchise_id = %s
    """
    return query_db(sql, (franchise_id,))


def get_invoices(franchise_id):
    """Get invoices for a franchise."""
    sql = """
        SELECT i.*, f.name AS franchise_name
        FROM invoices i
        LEFT JOIN franchises f ON f.id = i.franchise_id
        WHERE i.franchise_id = %s
        ORDER BY i.created_at DESC
    """
    return query_db(sql, (franchise_id,))


def get_invoice_by_reference(invoice_reference):
    """Get an invoice by its reference."""
    sql = """
        SELECT i.*, f.name AS franchise_name
        FROM invoices i
        LEFT JOIN franchises f ON f.id = i.franchise_id
        WHERE i.reference = %s
    """
    return query_db(sql, (invoice_reference,), one=True)


def get_payments_for_franchise(franchise_id):
    """Get payments for a franchise."""
    sql = """
        SELECT p.*, f.name AS franchise_name
        FROM payments p
        LEFT JOIN franchises f ON f.id = p.franchise_id
        WHERE p.franchise_id = %s
        ORDER BY p.created_at DESC
    """
    return query_db(sql, (franchise_id,))


def get_revenue_summary(franchise_id):
    """Get revenue summary for a franchise."""
    sql = """
        SELECT
            SUM(CASE WHEN i.status = 'paid' THEN i.amount ELSE 0 END) AS total_revenue,
            SUM(CASE WHEN i.status = 'paid' THEN i.base_amount ELSE 0 END) AS total_base_amount,
            SUM(CASE WHEN i.status = 'paid' THEN i.usage_amount ELSE 0 END) AS total_usage_amount,
            COUNT(*) AS total_invoices,
            COUNT(CASE WHEN i.status = 'paid' THEN 1 END) AS paid_invoices,
            COUNT(CASE WHEN i.status = 'unpaid' THEN 1 END) AS unpaid_invoices
        FROM invoices i
        WHERE i.franchise_id = %s
    """
    return query_db(sql, (franchise_id,), one=True)


def get_monthly_revenue(franchise_id, year_month):
    """Get monthly revenue for a franchise and year-month."""
    sql = """
        SELECT
            SUM(CASE WHEN i.status = 'paid' THEN i.amount ELSE 0 END) AS total_revenue,
            SUM(CASE WHEN i.status = 'paid' THEN i.base_amount ELSE 0 END) AS total_base_amount,
            SUM(CASE WHEN i.status = 'paid' THEN i.usage_amount ELSE 0 END) AS total_usage_amount,
            COUNT(*) AS total_invoices,
            COUNT(CASE WHEN i.status = 'paid' THEN 1 END) AS paid_invoices,
            COUNT(CASE WHEN i.status = 'unpaid' THEN 1 END) AS unpaid_invoices
        FROM invoices i
        WHERE i.franchise_id = %s AND i.billing_period = %s
    """
    return query_db(sql, (franchise_id, year_month), one=True)


def get_last_payment(franchise_id):
    """Get the most recent payment (or billing record) for a franchise."""
    sql = """
        SELECT *
        FROM billing_records
        WHERE franchise_id = %s
        ORDER BY paid_at DESC, updated_at DESC
        LIMIT 1
    """
    return query_db(sql, (franchise_id,), one=True)


def get_billing_record_by_id(bidding_id):
    """Get a billing record by its ID."""
    sql = "SELECT * FROM billing_records WHERE id = %s"
    return query_db(sql, (bidding_id,), one=True)


def get_financial_dashboard(franchise_id):
    """Get financial dashboard data for a franchise.
    This method is intentionally left empty to be implemented in the service layer
    by composing other repository methods.
    """
    # We'll return an empty dict; the service will build the dashboard.
    return {}


def get_available_features(franchise_id):
    """Get available features for a franchise."""
    flags = get_feature_flags(franchise_id)
    if not flags:
        return []
    return [f["feature_key"] for f in flags if f.get("enabled")]


def get_plan_limits(plan_code):
    """Get plan limits for a plan code."""
    from platform_helpers import PLAN_DEFINITIONS
    plan = PLAN_DEFINITIONS.get(plan_code.lower(), PLAN_DEFINITIONS["basic"])
    return {
        "branch_limit": plan["branch_limit"],
        "user_limit": plan["user_limit"],
        "automation_enabled": plan["automation_enabled"],
        "chatbot_enabled": plan["chatbot_enabled"],
        "reporting_enabled": plan["reporting_enabled"],
        "custom_integrations_enabled": plan["custom_integrations_enabled"],
        "priority_support_enabled": plan["priority_support_enabled"],
    }


def get_billing_statistics(franchise_id):
    """Get billing statistics for a franchise."""
    # We'll implement this in the service by calling other methods.
    pass