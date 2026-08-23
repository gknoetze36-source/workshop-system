"""
Financial Repository for Workshop System Version 2.

This repository handles all database operations for the Financial entity.
"""

from database import query_db


# ============================================================================
# Shared SQL
# ============================================================================


_INVOICE_SELECT = """
    SELECT
        i.*,
        f.name AS location_name
    FROM invoices i
    LEFT JOIN locations f
        ON f.id = i.location_id
"""

_PAYMENT_SELECT = """
    SELECT
        p.*,
        f.name AS location_name
    FROM payments p
    LEFT JOIN locations f
        ON f.id = p.location_id
"""

_REVENUE_SUMMARY = """
    SELECT
        SUM(CASE WHEN i.status='paid' THEN i.amount ELSE 0 END) AS total_revenue,
        SUM(CASE WHEN i.status='paid' THEN i.base_amount ELSE 0 END) AS total_base_amount,
        SUM(CASE WHEN i.status='paid' THEN i.usage_amount ELSE 0 END) AS total_usage_amount,
        COUNT(*) AS total_invoices,
        COUNT(CASE WHEN i.status='paid' THEN 1 END) AS paid_invoices,
        COUNT(CASE WHEN i.status='unpaid' THEN 1 END) AS unpaid_invoices
    FROM invoices i
"""



# ============================================================================
# Location
# ============================================================================

def get_subscription_status(location_id):
    """Return subscription status."""

    return query_db(
        """
        SELECT subscription_status, active
        FROM locations
        WHERE id=%s
        """,
        (location_id,),
        one=True,
    )


def get_location(location_id):
    """Return a location."""

    return query_db(
        "SELECT * FROM locations WHERE id=%s",
        (location_id,),
        one=True,
    )


def get_feature_flags(location_id):
    """Return feature flags."""

    return query_db(
        """
        SELECT feature_key, enabled
        FROM feature_flags
        WHERE location_id=%s
        """,
        (location_id,),
    )


# ============================================================================
# Invoices
# ============================================================================

def get_invoices(location_id):
    """Return invoices."""

    sql = f"""
        {_INVOICE_SELECT}
        WHERE i.location_id=%s
        ORDER BY i.created_at DESC
    """

    return query_db(sql, (location_id,))


def get_invoice_by_reference(invoice_reference):
    """Return an invoice by reference."""

    sql = f"""
        {_INVOICE_SELECT}
        WHERE i.reference=%s
    """

    return query_db(sql, (invoice_reference,), one=True)


# ============================================================================
# Payments
# ============================================================================

def get_payments_for_location(location_id):
    """Return payments."""

    sql = f"""
        {_PAYMENT_SELECT}
        WHERE p.location_id=%s
        ORDER BY p.created_at DESC
    """

    return query_db(sql, (location_id,))


def get_last_payment(location_id):
    """Return the latest payment."""

    return query_db(
        """
        SELECT *
        FROM billing_records
        WHERE location_id=%s
        ORDER BY paid_at DESC, updated_at DESC
        LIMIT 1
        """,
        (location_id,),
        one=True,
    )


def get_billing_record_by_id(billing_id):
    """Return a billing record."""

    return query_db(
        "SELECT * FROM billing_records WHERE id=%s",
        (billing_id,),
        one=True,
    )


# ============================================================================
# Revenue
# ============================================================================

def get_revenue_summary(location_id):
    """Return revenue summary."""

    sql = f"""
        {_REVENUE_SUMMARY}
        WHERE i.location_id=%s
    """

    return query_db(sql, (location_id,), one=True)


def get_monthly_revenue(location_id, year_month):
    """Return monthly revenue."""

    sql = f"""
        {_REVENUE_SUMMARY}
        WHERE i.location_id=%s
          AND i.billing_period=%s
    """

    return query_db(
        sql,
        (location_id, year_month),
        one=True,
    )
