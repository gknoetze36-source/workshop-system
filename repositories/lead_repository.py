"""
Lead Repository for Workshop System Version 2.

This repository handles all database operations for the Lead entity.
It interacts with the existing 'leads' table.
"""

from database import query_db, utc_now, execute_db, get_connection, transaction


# ============================================================================
# Persistence Operations
# ============================================================================

def attach_lead_to_customer(lead_id, customer_id, location_id):
    """
    Attach a lead to a customer by updating the customer's lead_id.
    """
    execute_db(
        """
        UPDATE customers
        SET lead_id = %s,
            updated_at = %s
        WHERE id = %s AND location_id = %s
        """,
        (lead_id, utc_now(), customer_id, location_id),
    )


def mark_lead_converted(lead_id, customer_id, location_id):
    """
    Mark a lead as converted by updating its status and customer_id.
    """
    execute_db(
        """
        UPDATE leads
        SET status = %s,
            customer_id = %s,
            updated_at = %s
        WHERE id = %s AND location_id = %s
        """,
        ("Converted", customer_id, utc_now(), lead_id, location_id),
    )


# ==================================################################==========
# Internal Helpers
# ============================================================================


def _get_lead_by_field(field, value):
    """
    Generic lead lookup helper.

    Args:
        field: Database column name.
        value: Value to search for.
    """
    allowed_fields = {
        "id",
    }

    if field not in allowed_fields:
        raise ValueError(f"Unsupported lead lookup field: {field}")

    sql = f"SELECT * FROM leads WHERE {field} = %s"

    return query_db(sql, (value,), one=True)


# ============================================================================
# Lead Lookups
# ============================================================================

def get_lead_by_id(lead_id):
    """Retrieve a lead by their ID."""
    return _get_lead_by_field("id", lead_id)


# ============================================================================
# Lead Queries
# ============================================================================

def get_leads(location_id, filters=None):
    """Retrieve leads for a location with optional filters."""
    sql = "SELECT * FROM leads WHERE location_id = %s"
    params = [location_id]
    if filters:
        if 'status' in filters:
            sql += " AND status = %s"
            params.append(filters['status'])
        if 'source' in filters:
            sql += " AND source = %s"
            params.append(filters['source'])
    sql += " ORDER BY created_at DESC"
    return query_db(sql, tuple(params))


def get_leads_by_status(location_id, status):
    """Retrieve leads for a location by status."""
    sql = "SELECT * FROM leads WHERE location_id = %s AND status = %s"
    return query_db(sql, (location_id, status))


def get_leads_by_source(location_id, source):
    """Retrieve leads for a source."""
    sql = "SELECT * FROM leads WHERE location_id = %s AND source = %s"
    return query_db(sql, (location_id, source))


# ============================================================================
# Lead Creation
# ============================================================================

def create_lead(data):
    """Create a new lead record and return its ID."""
    now = utc_now()
    params = (
        # location_id was passed twice -- a franchise-era leftover that made
        # this supply 9 values for 8 columns, so every lead insert raised.
        data["location_id"],
        data["contact_name"],
        data.get("contact_phone"),
        data.get("contact_email"),
        data["source"],
        data.get("status", "New"),
        now,
        now,
    )
    conn, backend = get_connection()
    conn.close()

    with transaction():
        if backend == "postgres":
            sql = """
                INSERT INTO leads (
                    location_id, contact_name, contact_phone,
                    contact_email, source, status, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                ) RETURNING id
                """
            result = query_db(sql, params)
            rid = result[0]["id"]
        else:
            sql = """
                INSERT INTO leads (
                    location_id, contact_name, contact_phone,
                    contact_email, source, status, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            query_db(sql, params)
            rid = query_db(
                "SELECT last_insert_rowid() AS id",
                one=True,
            )["id"]

    return rid


# ============================================================================
# Lead Updates
# ============================================================================

def update_lead(lead_id, data):
    """Update an existing lead."""
    # Build SET clause dynamically based on provided fields
    allowed = {"contact_name", "contact_phone", "contact_email", "source", "status"}
    sets = []
    vals = []
    for field in allowed:
        if field in data:
            sets.append(f"{field} = %s")
            vals.append(data[field])
    if not sets:
        return
    sets.append("updated_at = %s")
    vals.append(utc_now())
    vals.append(lead_id)
    sql = f"UPDATE leads SET {', '.join(sets)} WHERE id = %s"
    query_db(sql, tuple(vals))


# ============================================================================
# Lead Deletion
# ============================================================================

def delete_lead(lead_id):
    """Delete a lead."""
    query_db(
        "DELETE FROM leads WHERE id = %s",
        (lead_id,),
    )