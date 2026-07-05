"""
Booking Service for Workshop System Version 2.

This service contains all business logic for the Booking entity.
It depends only on the Booking Repository.
"""

from repositories.booking_repository import (
    get_visible_bookings as _get_visible_bookings,
    get_booking_by_reference as _get_booking_by_reference,
    get_booking_by_reference_raw as _get_booking_by_reference_raw,
    get_booking_by_id as _get_booking_by_id,
    get_booking_count_per_branch as _get_booking_count_per_branch,
    get_bookings_for_customers as _get_bookings_for_customers,
    get_bookings_for_customer_history as _get_bookings_for_customer_history,
    get_booking_service_history_by_vin_and_franchise as _get_booking_service_history_by_vin_and_franchise,
    get_booking_count_by_branch_and_date as _get_booking_count_by_branch_and_date,
)


def get_visible_bookings(user, filters=None):
    """Get visible bookings with optional filters."""
    return _get_visible_bookings(user, filters)


def get_booking_by_reference(reference, user):
    """Get a booking by its reference and user scope."""
    return _get_booking_by_reference(reference, user)


def get_booking_by_reference_raw(reference):
    """Get a booking by reference without any user scope (for public use)."""
    return _get_booking_by_reference_raw(reference)


def get_booking_by_id(booking_id):
    """Get a booking by its ID."""
    return _get_booking_by_id(booking_id)


def get_booking_count_per_branch():
    """Get the count of bookings per branch."""
    return _get_booking_count_per_branch()


def get_bookings_for_customers(user):
    """Get bookings for the customers list (based on user's role)."""
    if user["role"] == "franchise_admin":
        clause, args = "b.franchise_id=%s", [user["franchise_id"]]
    elif user["role"] == "reception":
        clause, args = "b.branch_id=%s", [user["branch_id"]]
    else:  # super_admin
        clause, args = "1=1", []
    return _get_bookings_for_customers(clause, args)


def get_bookings_for_customer_history(user, phone):
    """Get bookings for customer history (based on user's role and phone)."""
    if user["role"] == "franchise_admin":
        clause, args = "b.franchise_id=%s", [user["franchise_id"]]
    elif user["role"] == "reception":
        clause, args = "b.branch_id=%s", [user["branch_id"]]
    else:  # super_admin
        clause, args = "1=1", []
    return _get_bookings_for_customer_history(clause, args, phone)


def get_booking_service_history_by_vin_and_franchise(vin, franchise_id):
    """Get service history for a vehicle by VIN and franchise."""
    return _get_booking_service_history_by_vin_and_franchise(vin, franchise_id)


def get_booking_count_by_branch_and_date(branch_id, date):
    """Get count of bookings for a specific branch and date."""
    return _get_booking_count_by_branch_and_date(branch_id, date)