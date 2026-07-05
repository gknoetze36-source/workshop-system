"""
Booking Repository for Workshop System Version 2.

This repository handles all database operations for the Booking entity.
It interacts with the existing 'bookings' table.
"""

from database import query_db


def _scope_clause(user):
    """Replicate the scope_clause logic from platform_helpers."""
    if user["role"] == "super_admin":
        return "1=1", []
    if user["role"] == "franchise_admin":
        return "b.franchise_id = %s", [user["franchise_id"]]
    return "b.branch_id = %s", [user["branch_id"]]


def get_visible_bookings(user, filters=None):
    """Get visible bookings with optional filters (replicates platform_helpers.fetch_visible_bookings)."""
    filters = filters or {}
    clause, args = _scope_clause(user)
    where = [clause]

    search = (filters.get("search") or "").strip().lower()
    if search:
        where.append(
            """
            (
                lower(COALESCE(b.booking_reference, '')) LIKE %s OR
                lower(COALESCE(b.first_name, '')) LIKE %s OR
                lower(COALESCE(b.surname, '')) LIKE %s OR
                lower(COALESCE(b.phone, '')) LIKE %s OR
                lower(COALESCE(b.make, '')) LIKE %s OR
                lower(COALESCE(b.model, '')) LIKE %s OR
                lower(COALESCE(b.service, '')) LIKE %s
            )
            """
        )
        args.extend([f"%{search}%"] * 7)

    if filters.get("status"):
        where.append("b.status = %s")
        args.append(filters["status"])

    if filters.get("scheduled_date"):
        where.append("b.scheduled_date = %s")
        args.append(filters["scheduled_date"])

    if filters.get("branch_id"):
        where.append("b.branch_id = %s")
        args.append(filters["branch_id"])

    if filters.get("franchise_id"):
        where.append("b.franchise_id = %s")
        args.append(filters["franchise_id"])

    where_clause = " AND ".join(where)
    sql = f"""
        SELECT
            b.*,
            f.name AS franchise_name,
            f.slug AS franchise_slug,
            br.name AS branch_name,
            br.slug AS branch_slug,
            br.contact_email AS branch_contact_email,
            br.contact_phone AS branch_contact_phone
        FROM bookings b
        LEFT JOIN franchises f ON f.id = b.franchise_id
        LEFT JOIN branches br ON br.id = b.branch_id
        WHERE {where_clause}
        ORDER BY b.scheduled_date ASC, b.created_at DESC
    """
    return query_db(sql, tuple(args))


def get_booking_by_reference(reference, user):
    """Get a booking by reference and user scope (replicates platform_helpers.fetch_booking_for_user)."""
    # Fetch by reference without any scope first
    sql = """
        SELECT
            b.*,
            f.name AS franchise_name,
            f.slug AS franchise_slug,
            br.name AS branch_name,
            br.slug AS branch_slug,
            br.contact_email AS branch_contact_email,
            br.contact_phone AS branch_contact_phone
        FROM bookings b
        LEFT JOIN franchises f ON f.id = b.franchise_id
        LEFT JOIN branches br ON br.id = b.branch_id
        WHERE b.booking_reference=%s
    """
    booking = query_db(sql, (reference,), one=True)
    if booking is None:
        return None
    # Now check scope (replicate booking_in_scope from platform_helpers)
    if user["role"] == "super_admin":
        return booking
    if user["role"] == "franchise_admin":
        if booking.get("franchise_id") == user["franchise_id"]:
            return booking
        else:
            return None
    # reception
    if booking.get("branch_id") == user["branch_id"]:
        return booking
    else:
        return None


def get_booking_by_reference_raw(reference):
    """Get a booking by reference without any user scope (for public use)."""
    sql = """
        SELECT
            b.*,
            f.name AS franchise_name,
            f.slug AS franchise_slug,
            br.name AS branch_name,
            br.slug AS branch_slug,
            br.contact_email AS branch_contact_email,
            br.contact_phone AS branch_contact_phone
        FROM bookings b
        LEFT JOIN franchises f ON f.id = b.franchise_id
        LEFT JOIN branches br ON br.id = b.branch_id
        WHERE b.booking_reference=%s
    """
    return query_db(sql, (reference,), one=True)


def get_booking_by_id(booking_id):
    """Get a booking by its ID (with franchise and branch details)."""
    sql = """
        SELECT
            b.*,
            f.name AS franchise_name,
            f.slug AS franchise_slug,
            br.name AS branch_name,
            br.slug AS branch_slug,
            br.contact_email AS branch_contact_email,
            br.contact_phone AS branch_contact_phone
        FROM bookings b
        LEFT JOIN franchises f ON f.id = b.franchise_id
        LEFT JOIN branches br ON br.id = b.branch_id
        WHERE b.id=%s
    """
    return query_db(sql, (booking_id,), one=True)


def get_booking_count_per_branch():
    """Get the count of bookings per branch."""
    sql = "SELECT branch_id, COUNT(*) AS total FROM bookings GROUP BY branch_id"
    return query_db(sql)


def get_bookings_for_customers(clause, args):
    """Get bookings for the customers list (used in the customers route)."""
    sql = f"""
        SELECT
            b.id,
            b.booking_reference,
            b.first_name,
            b.surname,
            b.customer_email,
            b.phone,
            b.work_to_be_done,
            b.internal_notes,
            br.name AS branch_name
        FROM bookings b
        LEFT JOIN branches br ON br.id = b.branch_id
        WHERE {clause}
        ORDER BY b.id DESC
    """
    return query_db(sql, tuple(args))


def get_bookings_for_customer_history(clause, args, phone):
    """Get bookings for customer history (used in the customer history route)."""
    sql = f"""
        SELECT
            b.booking_reference,
            b.scheduled_date,
            b.service,
            b.status,
            br.name AS branch_name
        FROM bookings b
        LEFT JOIN branches br ON br.id = b.branch_id
        WHERE {clause}
          AND COALESCE(b.phone, '')=%s
        ORDER BY b.id DESC
    """
    return query_db(sql, tuple(args) + (phone,))


def get_booking_service_history_by_vin_and_franchise(vin, franchise_id):
    """Get service history for a vehicle by VIN and franchise."""
    sql = """
        SELECT b.id, b.service, b.scheduled_date, b.status,
               b.current_mileage, b.work_to_be_done
        FROM bookings b
        WHERE b.vehicle_vin = %s AND b.franchise_id = %s
        ORDER BY b.scheduled_date DESC
    """
    return query_db(sql, (vin, franchise_id))


def get_booking_count_by_branch_and_date(branch_id, date):
    """Get count of bookings for a specific branch and date."""
    sql = """
        SELECT COUNT(*) as total
        FROM bookings
        WHERE branch_id=%s AND scheduled_date=%s
    """
    return query_db(sql, (branch_id, date))