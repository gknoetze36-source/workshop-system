"""
Inquiry Repository for Workshop System Version 2.

This repository handles all database operations for the Booking Inquiries entity.
It interacts with the existing 'booking_inquiries' table.
"""

from database import query_db


# ============================================================================
# Inquiry Queries
# ============================================================================

def find_active_inquiry(location_id, phone="", email=""):
    """
    Find an active inquiry for a location/location by phone or email.
    """
    phone = (phone or "").strip()
    email = (email or "").strip().lower()

    if phone:
        inquiry = query_db(
            """
            SELECT *
            FROM booking_inquiries
            WHERE location_id=%s
              AND customer_phone=%s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (location_id, phone),
            one=True,
        )
        if inquiry:
            return inquiry

    if email:
        return query_db(
            """
            SELECT *
            FROM booking_inquiries
            WHERE location_id=%s
              AND lower(COALESCE(customer_email, ''))=lower(%s)
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (location_id, email),
            one=True,
        )

    return None


def fetch_inquiries_for_user(user, limit=30):
    """
    Fetch inquiries for a user with proper scoping.
    """
    from helpers.common import scope_clause

    clause, args = scope_clause(user, alias="bi")
    return query_db(
        f"""
        SELECT
            bi.*,
            b.booking_reference,
            br.name AS location_name
        FROM booking_inquiries bi
        LEFT JOIN bookings b ON b.id = bi.booking_id
        LEFT JOIN locations br ON br.id = bi.location_id
        WHERE {clause}
        ORDER BY bi.updated_at DESC, bi.created_at DESC
        LIMIT %s
        """,
        tuple(args + [limit]),
    ) or []


def inquiry_metrics(user):
    """
    Get inquiry metrics for a user.
    """
    from helpers.common import scope_clause

    clause, args = scope_clause(user, alias="bi")
    row = query_db(
        f"""
        SELECT
            COUNT(*) AS total_inquiries,
            SUM(CASE WHEN bi.user_state='NEW_INQUIRY' THEN 1 ELSE 0 END) AS new_inquiries,
            SUM(CASE WHEN bi.user_state='ENGAGED' THEN 1 ELSE 0 END) AS engaged_inquiries,
            SUM(CASE WHEN bi.user_state='BOOKING_PENDING' THEN 1 ELSE 0 END) AS booking_pending,
            SUM(CASE WHEN bi.user_state='BOOKED' THEN 1 ELSE 0 END) AS booked_inquiries,
            SUM(CASE WHEN bi.user_state='LOST' THEN 1 ELSE 0 END) AS lost_inquiries,
            SUM(COALESCE(bi.followups_sent_count, 0)) AS followups_sent,
            SUM(COALESCE(bi.replies_after_followup_count, 0)) AS replies_after_followup,
            SUM(COALESCE(bi.bookings_from_followups_count, 0)) AS bookings_from_followups
        FROM booking_inquiries bi
        WHERE {clause}
        """,
        tuple(args),
    ) or {}

    return {key: int(row.get(key) or 0) for key in [
        "total_inquiries",
        "new_inquiries",
        "engaged_inquiries",
        "booking_pending",
        "booked_inquiries",
        "lost_inquiries",
        "followups_sent",
        "replies_after_followup",
        "bookings_from_followups",
    ]}