"""
Vehicle Repository for Workshop System Version 2.

This repository handles all database operations for the Vehicle entity.
It interacts with the existing 'vehicles' table.
"""

from database import query_db


def get_vehicles_by_franchise(franchise_id):
    """Get all vehicles for a franchise with customer name."""
    sql = """
        SELECT v.*, c.full_name as customer_name
        FROM vehicles v
        JOIN customers c ON v.customer_id = c.id
        WHERE v.franchise_id = %s
        ORDER BY v.updated_at DESC
    """
    return query_db(sql, (franchise_id,))


def get_vehicle_by_id(vehicle_id, franchise_id):
    """Get a vehicle by ID and franchise ID with customer details."""
    sql = """
        SELECT v.*, c.full_name as customer_name, c.phone, c.email
        FROM vehicles v
        JOIN customers c ON v.customer_id = c.id
        WHERE v.id = %s AND v.franchise_id = %s
    """
    return query_db(sql, (vehicle_id, franchise_id), one=True)


def get_vehicle_by_vin(vin, franchise_id):
    """Get a vehicle by VIN and franchise ID with customer details."""
    sql = """
        SELECT v.*, c.full_name as customer_name, c.phone, c.email
        FROM vehicles v
        JOIN customers c ON v.customer_id = c.id
        WHERE v.vehicle_vin = %s AND v.franchise_id = %s
    """
    return query_db(sql, (vin, franchise_id), one=True)


def get_vehicle_by_registration(registration, franchise_id):
    """Get a vehicle by registration/license plate and franchise ID with customer details."""
    sql = """
        SELECT v.*, c.full_name as customer_name, c.phone, c.email
        FROM vehicles v
        JOIN customers c ON v.customer_id = c.id
        WHERE v.license_plate = %s AND v.franchise_id = %s
    """
    return query_db(sql, (registration, franchise_id), one=True)


def get_vehicle_count(franchise_id):
    """Return the number of vehicles in a franchise."""
    sql = "SELECT COUNT(*) AS total FROM vehicles WHERE franchise_id = %s"
    result = query_db(sql, (franchise_id,), one=True)
    return result['total'] if result else 0


def get_recent_vehicles(franchise_id, limit=10):
    """Retrieve recent vehicles for a franchise."""
    sql = """
        SELECT v.*, c.full_name as customer_name
        FROM vehicles v
        JOIN customers c ON v.customer_id = c.id
        WHERE v.franchise_id = %s
        ORDER BY v.updated_at DESC
        LIMIT %s
    """
    return query_db(sql, (franchise_id, limit))