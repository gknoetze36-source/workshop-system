from database.query import fetch_one, fetch_all

"""
Common Helper Functions for Workshop System Version 2.

Shared helper functions used across multiple services.
"""

ROLE_LABELS = {
    "reception": "Reception",
    "location_admin": "Location Admin",
    "super_admin": "Platform Super Admin",
    "location_manager": "Location Manager",
    "technician": "Technician",
    "accounts": "Accounts",
    "viewer": "Viewer",
}


def boolish(value):
    """Convert common truthy values to bool."""
    return str(value).lower() in {"1", "true", "yes", "on"}


def db_bool(value):
    """Normalize database boolean values."""
    return value if isinstance(value, bool) else boolish(value)


def scope_clause(user, alias="b"):
    """Temporary compatibility wrapper."""
    if user["role"] == "super_admin":
        return "1=1", []
    if user["role"] in {"location_admin", "location_owner"}:
        return f"{alias}.location_id = %s", [user["location_id"]]

    location_id = user.get("location_id") or user.get("location_id")
    return f"{alias}.location_id = %s", [location_id]


