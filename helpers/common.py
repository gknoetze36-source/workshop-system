from database.query import fetch_one, fetch_all

"""
Common Helper Functions for Workshop System Version 2.

Shared helper functions used across multiple services.

ROLE_LABELS previously listed location_admin, location_manager, accounts and
viewer -- names no route ever accepted, so a user given one of them could not
reach any page. The display labels are now derived from the canonical role
vocabulary in helpers/permission.py, which is the only place role names are
defined. Imported lazily to avoid a circular import at module load.
"""


def _canonical_role_labels():
    from helpers.permission import ALL_USER_ROLES, PLATFORM_ROLES

    labels = {
        "owner": "Owner",
        "admin": "Administrator",
        "manager": "Manager",
        "reception": "Reception",
        "technician": "Technician",
        "readonly": "Read Only",
    }
    result = {role: labels.get(role, role.replace("_", " ").title()) for role in ALL_USER_ROLES}
    for role in sorted(PLATFORM_ROLES):
        result[role] = "Platform Super Admin" if role == "super_admin" else role.replace("_", " ").title()
    return result


ROLE_LABELS = _canonical_role_labels()


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


