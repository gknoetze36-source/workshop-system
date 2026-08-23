"""
Customer Repository for Workshop System Version 2.

This repository handles all database operations for the Customer entity.
It interacts with the existing 'customers' table.
"""

from database.query import query_db
from database.utils import execute_db, utc_now


# ============================================================================
# Internal Helpers
# ============================================================================

def _get_customer_by_field(field, value, location_id=None):
    """
    Generic customer lookup helper.

    Args:
        field: Database column name.
        value: Value to search for.
        location_id: Optional location restriction.
    """
    allowed_fields = {
        "id",
        "phone",
        "email",
    }

    if field not in allowed_fields:
        raise ValueError(f"Unsupported customer lookup field: {field}")

    sql = f"SELECT * FROM customers WHERE {field}"

    params = []

    if field == "email":
        sql += " = lower(%s)"
        sql = sql.replace("email", "lower(email)")
    else:
        sql += " = %s"

    params.append(value)

    if location_id is not None:
        sql += " AND location_id = %s"
        params.append(location_id)

    sql += " ORDER BY id DESC LIMIT 1"

    return query_db(sql, tuple(params), one=True)


# ============================================================================
# Customer Lookups
# ============================================================================

def get_customer_by_id(customer_id, location_id):
    """Retrieve a customer by ID within the authenticated location."""
    return _get_customer_by_field("id", customer_id, location_id)


def get_customer_by_id_and_location(customer_id, location_id):
    """Retrieve a customer by ID within a location."""
    return _get_customer_by_field(
        "id",
        customer_id,
        location_id,
    )


def get_customer_by_phone_and_location(phone, location_id):
    """Retrieve the latest customer by phone."""
    return _get_customer_by_field(
        "phone",
        phone,
        location_id,
    )


def get_customer_by_email_and_location(email, location_id):
    """Retrieve the latest customer by email."""
    return _get_customer_by_field(
        "email",
        email,
        location_id,
    )


# ============================================================================
# Statistics
# ============================================================================

def get_customer_count_by_location(location_id):
    """Return the total number of customers in a location."""
    sql = """
        SELECT COUNT(*) AS total
        FROM customers
        WHERE location_id = %s
    """

    result = query_db(sql, (location_id,), one=True)

    return result["total"] if result else 0


# ============================================================================
# Persistence Operations
# ============================================================================



def create_customer(location_id, first_name, surname, full_name, phone, email, accepts_whatsapp):
    """Create a new customer record and return the customer ID."""
    now = utc_now()

    execute_db(
        """
        INSERT INTO customers (
            location_id, location_id, first_name, surname, last_name, full_name, phone, whatsapp_number, email,
            accepts_whatsapp, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (location_id, location_id, first_name, surname, surname, full_name, phone, phone, email, accepts_whatsapp, now, now),
    )

    # Return the ID of the newly created customer
    if phone:
        row = get_customer_by_phone_and_location(phone, location_id)
    else:
        row = get_customer_by_email_and_location(email, location_id)
    return row["id"] if row else None


def update_customer(customer_id, location_id, first_name=None, surname=None, full_name=None, phone=None, email=None, accepts_whatsapp=None):
    """Update an existing customer record."""
    # Build the SET clause dynamically based on provided fields
    sets = []
    params = []

    if first_name is not None:
        sets.append("first_name = %s")
        params.append(first_name)

    if surname is not None:
        sets.append("surname = %s")
        params.append(surname)

    if full_name is not None:
        sets.append("full_name = %s")
        params.append(full_name)

    if phone is not None:
        sets.append("phone = %s")
        params.append(phone)

    if email is not None:
        sets.append("email = %s")
        params.append(email)

    if accepts_whatsapp is not None:
        sets.append("accepts_whatsapp = %s")
        params.append(accepts_whatsapp)

    if not sets:
        return

    sets.append("updated_at = %s")
    params.append(utc_now())
    params.extend([customer_id, location_id])

    sql = f"UPDATE customers SET {', '.join(sets)} WHERE id = %s AND location_id = %s"
    execute_db(sql, tuple(params))