"""
Booking Repository for Workshop System Version 2.

This repository handles all database operations for the Booking entity.
It interacts with the existing 'bookings' table.
"""

from database import query_db


# ============================================================================
# Internal Helpers
# ============================================================================

def _scope_clause(user):
    """
    Build the SQL location scope for the current user.

    Returns:
        tuple[str, list]
        (sql_clause, parameters)
    """
    role = user.get("role")

    if role == "super_admin":
        return "1=1", []

    if role in {"owner", "location_admin"}:
        return "b.location_id = %s", [user["location_id"]]

    location_id = user.get("location_id")
    return "b.location_id = %s", [location_id]


_BASE_BOOKING_SELECT = """
SELECT
    b.*,
    l.name AS location_name,
    l.slug AS location_slug,
    l.contact_email AS location_contact_email,
    l.contact_phone AS location_contact_phone
FROM bookings b
LEFT JOIN locations l ON l.id = b.location_id
"""
# ============================================================================
# Booking Queries
# ============================================================================

def find_duplicate_booking(
    location_id,
    phone,
    vin,
    registration_number,
    scheduled_date,
):
    sql = """
    SELECT id,
           booking_reference
    FROM bookings
    WHERE location_id=%s
      AND scheduled_date=%s
      AND status NOT IN ('Cancelled','Collected')
      AND (
            phone=%s
         OR vehicle_vin=%s
      )
    LIMIT 1
    """

    return query_db(
        sql,
        (
            location_id,
            scheduled_date,
            phone,
            vin,
            registration_number,
        ),
        one=True,
    )


def get_visible_bookings(user, filters=None):
    """
    Get bookings visible to the supplied user with optional filtering.
    """
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

    if filters.get("location_id"):
        where.append("b.location_id = %s")
        args.append(filters["location_id"])

    sql = (
        _BASE_BOOKING_SELECT
        + f"""
WHERE {' AND '.join(where)}
ORDER BY b.scheduled_date ASC,
         b.created_at DESC
"""
    )

    return query_db(sql, tuple(args))


def get_booking_by_reference(reference, user):
    """
    Get a booking by reference while enforcing location scope in SQL.
    """
    clause, args = _scope_clause(user)

    sql = (
        _BASE_BOOKING_SELECT
        + f"""
WHERE b.booking_reference = %s
AND {clause}
"""
    )

    return query_db(sql, (reference, *args), one=True)


def get_booking_by_reference_raw(reference):
    """
    Public lookup by booking reference.
    No location filtering is applied.
    """
    sql = (
        _BASE_BOOKING_SELECT
        + """
WHERE b.booking_reference = %s
"""
    )

    return query_db(sql, (reference,), one=True)


def get_booking_by_id(booking_id, location_id):
    """Get a booking by ID within a location."""
    sql = (
        _BASE_BOOKING_SELECT
        + """
WHERE b.id = %s
  AND b.location_id = %s
"""
    )
    return query_db(sql, (booking_id, location_id), one=True)


def get_booking_by_id_for_user(booking_id, user):
    """
    Get a booking by ID while enforcing location scope.
    """
    clause, args = _scope_clause(user)

    sql = (
        _BASE_BOOKING_SELECT
        + f"""
WHERE b.id = %s
AND {clause}
"""
    )

    return query_db(sql, (booking_id, *args), one=True)

# ============================================================================
# Reporting Queries
# ============================================================================

def get_booking_count_per_location():
    """
    Get the total number of bookings per location.
    """
    sql = """
        SELECT
            location_id,
            COUNT(*) AS total
        FROM bookings
        GROUP BY location_id
    """
    return query_db(sql)


# ============================================================================
# Customer Queries
# ============================================================================

def get_bookings_for_customers(clause, args):
    """
    Get bookings for the customer listing.

    NOTE:
    The caller is responsible for constructing a safe SQL clause.
    This design should be revisited after service-layer verification.
    """
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
            br.name AS location_name
        FROM bookings b
        LEFT JOIN locations br
            ON br.id = b.location_id
        WHERE {clause}
        ORDER BY b.id DESC
    """

    return query_db(sql, tuple(args))


def get_bookings_for_customer_history(clause, args, phone):
    """
    Get booking history for a customer.

    NOTE:
    The caller is responsible for constructing a safe SQL clause.
    This design should be revisited after service-layer verification.
    """
    sql = f"""
        SELECT
            b.booking_reference,
            b.scheduled_date,
            b.service,
            b.status,
            br.name AS location_name
        FROM bookings b
        LEFT JOIN locations br
            ON br.id = b.location_id
        WHERE {clause}
          AND COALESCE(b.phone, '') = %s
        ORDER BY b.id DESC
    """

    return query_db(sql, (*args, phone))


# ============================================================================
# Vehicle History
# ============================================================================

def get_booking_service_history_by_vin_and_location(vin, location_id):
    """
    Get a vehicle's service history for a location.
    """
    sql = """
        SELECT
            b.id,
            b.service,
            b.scheduled_date,
            b.status,
            b.current_mileage,
            b.work_to_be_done
        FROM bookings b
        WHERE b.vehicle_vin = %s
          AND b.location_id = %s
        ORDER BY b.scheduled_date DESC
    """

    return query_db(sql, (vin, location_id))


# ============================================================================
# Statistics
# ============================================================================

def get_booking_count_by_location_and_date(location_id, date):
    """
    Get the number of bookings for a location on a specific date.
    """
    sql = """
        SELECT
            COUNT(*) AS total
        FROM bookings
        WHERE location_id = %s
          AND scheduled_date = %s
    """

    return query_db(sql, (location_id, date))


# ============================================================================
# Persistence Operations
# ============================================================================

def create_booking(booking_data):
    """
    Create a new booking record and return the booking reference.
    booking_data should contain all required fields for a booking.
    """
    from database import execute_db
    from datetime import datetime, time, timedelta

    scheduled = booking_data.get("scheduled_date") or booking_data.get("date")
    try:
        start_time = datetime.combine(datetime.fromisoformat(str(scheduled)).date(), time(8, 0))
    except (TypeError, ValueError):
        start_time = datetime.now()
    end_time = start_time + timedelta(hours=1)

    execute_db(
        """
        INSERT INTO bookings (
            booking_reference,
            location_id,
            company,
            location,
            customer_id,
            vehicle_id,
            service_id,
            first_name,
            surname,
            customer_email,
            phone,
            preferred_contact_method,
            make,
            model,
            vehicle_year,
            fuel_type,
            vehicle_vin,
            service,
            service_level,
            service_type,
            current_mileage,
            start_time,
            end_time,
            scheduled_date,
            date,
            status,
            service_due_date,
            work_to_be_done,
            public_notes,
            internal_notes,
            source,
            quote_declined,
            contacted,
            whatsapp_opt_in,
            privacy_consent_at,
            reminder_opt_in,
            completed_at,
            created_at,
            updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s
        )
        """,
        (
            booking_data["booking_reference"],
            booking_data["location_id"],
            booking_data["company"],
            booking_data["location"],
            booking_data["customer_id"],
            booking_data.get("vehicle_id"),
            booking_data["service_id"],
            booking_data["first_name"],
            booking_data["surname"],
            booking_data["customer_email"],
            booking_data["phone"],
            booking_data["preferred_contact_method"],
            booking_data["make"],
            booking_data["model"],
            booking_data["vehicle_year"],
            booking_data["fuel_type"],
            booking_data["vehicle_vin"],
            booking_data["service"],
            booking_data["service_level"],
            booking_data.get("service") or booking_data.get("service_level") or "Service",
            booking_data["current_mileage"],
            start_time,
            end_time,
            booking_data["scheduled_date"],
            booking_data["date"],
            booking_data["status"],
            booking_data["service_due_date"],
            booking_data["work_to_be_done"],
            booking_data["public_notes"],
            booking_data["internal_notes"],
            booking_data["source"],
            booking_data["quote_declined"],
            booking_data["contacted"],
            booking_data["whatsapp_opt_in"],
            booking_data["privacy_consent_at"],
            booking_data["reminder_opt_in"],
            booking_data["completed_at"],
            booking_data["created_at"],
            booking_data["updated_at"]
        ),
    )

    return booking_data["booking_reference"]


def attach_inquiry_to_booking(booking_reference, inquiry_id, followup_bookings, now_timestamp):
    """
    Attach an inquiry to a booking by updating the booking_inquiries table.
    """
    from database import execute_db

    execute_db(
        """
        UPDATE booking_inquiries
        SET booking_id=(
                SELECT id
                FROM bookings
                WHERE booking_reference=%s
            ),
            user_state='BOOKED',
            bookings_from_followups_count=
                COALESCE(bookings_from_followups_count, 0) + %s,
            stop_reason='booking_created',
            closed_at=%s,
            next_followup_at=NULL,
            updated_at=%s
        WHERE id=%s
          AND EXISTS (
              SELECT 1
              FROM bookings b
              WHERE b.booking_reference=%s
                AND b.location_id=booking_inquiries.location_id
          )
        """,
        (
            booking_reference,
            followup_bookings,
            now_timestamp,
            now_timestamp,
            inquiry_id,
            booking_reference,
        ),
    )



def generate_booking_reference(scheduled_date, location_id):
    prefix = f"BK-{(scheduled_date or utc_today()).replace('-', '')}"

    row = _fetch_one(
        """
        SELECT booking_reference
        FROM bookings
        WHERE booking_reference LIKE %s
          AND location_id = %s
        ORDER BY booking_reference DESC
        LIMIT 1
        """,
        (f"{prefix}-%",),
    )
    if row:
        last = int(row["booking_reference"].split("-")[-1])
        next_number = last + 1

    else:
        next_number = 1

    return f"{prefix}-{next_number:04d}"