"""
Vehicle Repository for Workshop System Version 2.

This repository handles all database operations for the Vehicle entity.
It interacts with the existing 'vehicles' table.
"""

from database import query_db


# ============================================================================
# Internal Helpers
# ============================================================================

_BASE_VEHICLE_SELECT = """
    SELECT
        v.*,
        c.full_name AS customer_name,
        c.phone,
        c.email
    FROM vehicles v
    JOIN customers c
        ON v.customer_id = c.id
"""


def _get_vehicle_by_field(field, value, location_id):
    """
    Generic vehicle lookup helper.
    """

    allowed_fields = {
        "id",
        "vehicle_vin",
        "license_plate",
    }

    if field not in allowed_fields:
        raise ValueError(f"Unsupported vehicle lookup field: {field}")

    sql = f"""
        {_BASE_VEHICLE_SELECT}
        WHERE v.{field} = %s
          AND v.location_id = %s
        LIMIT 1
    """

    return query_db(
        sql,
        (value, location_id),
        one=True,
    )


# ============================================================================
# Vehicle Queries
# ============================================================================

def get_vehicles_by_location(location_id):
    """Return all vehicles for a location."""

    sql = f"""
        {_BASE_VEHICLE_SELECT}
        WHERE v.location_id = %s
        ORDER BY v.updated_at DESC
    """

    return query_db(sql, (location_id,))


def get_vehicle_by_id(vehicle_id, location_id):
    """Return a vehicle by its ID."""
    return _get_vehicle_by_field(
        "id",
        vehicle_id,
        location_id,
    )


def get_vehicle_by_vin(vin, location_id):
    """Return a vehicle by VIN."""
    return _get_vehicle_by_field(
        "vehicle_vin",
        vin,
        location_id,
    )


def get_vehicle_by_registration(registration, location_id):
    """Return a vehicle by registration."""
    return _get_vehicle_by_field(
        "license_plate",
        registration,
        location_id,
    )


# ============================================================================
# Statistics
# ============================================================================

def get_vehicle_count(location_id):
    """Return the total number of vehicles in a location."""

    sql = """
        SELECT COUNT(*) AS total
        FROM vehicles
        WHERE location_id = %s
    """

    result = query_db(
        sql,
        (location_id,),
        one=True,
    )

    return result["total"] if result else 0


def get_recent_vehicles(location_id, limit=10):
    """Return the most recently updated vehicles."""

    sql = f"""
        {_BASE_VEHICLE_SELECT}
        WHERE v.location_id = %s
        ORDER BY v.updated_at DESC
        LIMIT %s
    """

    return query_db(
        sql,
        (location_id, limit),
    )