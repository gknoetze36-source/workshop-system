"""Canonical owner/location authorization helpers.

Business users are scoped to the single Location attached to their authenticated
Owner account. Platform administrators are the only users allowed to operate
outside a business Location.
"""

PLATFORM_ROLES = {"super_admin", "phanta_admin", "platform_admin"}

# These are the business roles exposed by the current onboarding/settings flows.
# Owner/admin users can create/manage operational Location users; platform
# administrators can create any role supported by those flows.
LOCATION_USER_ROLES = ("manager", "reception", "technician", "readonly")
ALL_USER_ROLES = ("owner", "admin", "manager", "reception", "technician", "readonly")


def assert_location_scope(row, user):
    """Return whether *row* belongs to the authenticated user's Location.

    ``row`` is expected to expose ``location_id`` and ``user`` is the
    authenticated session user. Platform administrators intentionally have no
    business Location and may operate across Locations. All other users must
    have a concrete Location and the row must belong to that same Location.

    This function is deliberately a boolean authorization predicate; callers
    can decide whether an unauthorized result should become a 403, 404, etc.
    """
    if not user or not row:
        return False

    if user.get("role") in PLATFORM_ROLES:
        return True

    user_location_id = user.get("location_id")
    row_location_id = row.get("location_id") if hasattr(row, "get") else None

    if not isinstance(user_location_id, int) or user_location_id <= 0:
        return False
    if not isinstance(row_location_id, int) or row_location_id <= 0:
        return False

    return row_location_id == user_location_id


def available_roles_for_creator(user):
    """Return the roles the authenticated user may create.

    The existing application exposes operational Location roles during
    onboarding/settings. A business Owner/Admin may create only those
    operational roles; platform administrators retain platform-level access.
    No franchise or branch role is introduced here.
    """
    if not user:
        return []

    role = user.get("role")
    if role in PLATFORM_ROLES:
        return list(ALL_USER_ROLES)
    if role in {"owner", "admin", "location_admin"}:
        return list(LOCATION_USER_ROLES)
    return []
