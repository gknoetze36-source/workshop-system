"""
Vehicle Service for Workshop System Version 2.

This service contains all business logic for the Vehicle entity.
It depends only on the Vehicle Repository.
"""

from repositories.vehicle_repository import (
    get_vehicles_by_franchise,
    get_vehicle_by_id,
    get_vehicle_by_vin,
    get_vehicle_by_registration,
    get_vehicle_count,
    get_recent_vehicles,
)


def get_vehicles_by_franchise(franchise_id):
    """Retrieve all vehicles for a given franchise."""
    return get_vehicles_by_franchise(franchise_id)


def get_vehicle_by_id(vehicle_id, franchise_id):
    """Retrieve a vehicle by its ID and franchise ID."""
    return get_vehicle_by_id(vehicle_id, franchise_id)


def get_vehicle_by_vin(vin, franchise_id):
    """Retrieve a vehicle by its VIN and franchise ID."""
    return get_vehicle_by_vin(vin, franchise_id)


def get_vehicle_by_registration(registration, franchise_id):
    """Retrieve a vehicle by its registration/license plate and franchise ID."""
    return get_vehicle_by_registration(registration, franchise_id)


def get_vehicle_count(franchise_id):
    """Return the number of vehicles in a franchise."""
    return get_vehicle_count(franchise_id)


def get_recent_vehicles(franchise_id, limit=10):
    """Retrieve recent vehicles for a franchise."""
    return get_recent_vehicles(franchise_id, limit)