"""
User Service for Workshop System Version 2.

This service contains all business logic for the User entity.
It depends only on the User Repository.
"""

from repositories.user_repository import (
    get_user_by_username_or_email as _get_user_by_username_or_email,
    get_user_by_username as _get_user_by_username,
    get_user_by_id as _get_user_by_id,
    get_user_count_by_location as _get_user_count_by_location,
    get_user_counts_by_location as _get_user_counts_by_location,
    get_users_with_filters as _get_users_with_filters,
    get_non_superadmin_excluding_current as _get_non_superadmin_excluding_current,
    get_all_users_ordered_by_location_name as _get_all_users_ordered_by_location_name,
    user_exists as _user_exists,
)


def get_user_by_username_or_email(username_or_email):
    """Retrieve a user by username or email (case-insensitive)."""
    return _get_user_by_username_or_email(username_or_email)


def get_user_by_username(username):
    """Retrieve a user by username (case-insensitive)."""
    return _get_user_by_username(username)


def get_user_by_id(user_id):
    """Retrieve a user by their ID."""
    return _get_user_by_id(user_id)


def get_user_count_by_location(location_id):
    """Return the number of active users in a location."""
    return _get_user_count_by_location(location_id)


def get_user_counts_by_location():
    """Return a list of user counts per location."""
    return _get_user_counts_by_location()


def get_users_with_filters(scope_sql, args):
    """
    Retrieve users with arbitrary scope SQL and args.
    Expected to be used with complex filtering (e.g., manage users).
    """
    return _get_users_with_filters(scope_sql, args)


def get_non_superadmin_excluding_current(current_username):
    """Return all non-superadmin users except the current user."""
    return _get_non_superadmin_excluding_current(current_username)


def get_all_users_ordered_by_location_name():
    """Return all users ordered by location name then username."""
    return _get_all_users_ordered_by_location_name()


def user_exists(username_or_email):
    """Check if a user exists by username or email."""
    return _user_exists(username_or_email)
