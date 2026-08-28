"""Canonical owner/location authorization helpers.

Business users are scoped to the single Location attached to their authenticated
Owner account. Platform administrators are the only users allowed to operate
outside a business Location.

ROLE VOCABULARY
---------------
This module is the single source of truth for role names. Three incompatible
vocabularies used to coexist: this file's ALL_USER_ROLES, helpers/common.py's
ROLE_LABELS (which carried location_admin / location_manager / accounts /
viewer -- none of which any route accepted), and ad-hoc inline sets written
directly into route handlers. A user created with one of the orphan labels
passed no route check anywhere and was locked out of the application.

helpers/common.py's ROLE_LABELS now derives from the names defined here, and
route handlers use @require_role instead of inline sets.
"""
from functools import wraps

from flask import flash, jsonify, redirect, request, session, url_for

PLATFORM_ROLES = {"super_admin", "phanta_admin", "platform_admin"}

# These are the business roles exposed by the current onboarding/settings flows.
# Owner/admin users can create/manage operational Location users; platform
# administrators can create any role supported by those flows.
LOCATION_USER_ROLES = ("manager", "reception", "technician", "readonly")
ALL_USER_ROLES = ("owner", "admin", "manager", "reception", "technician", "readonly")

# Roles that may change configuration, billing and users.
ADMIN_ROLES = frozenset({"owner", "admin"})
# Roles that may change configuration but not users/billing.
MANAGER_ROLES = frozenset({"owner", "admin", "manager"})
# Roles that run day-to-day operations: bookings, messaging, vehicle lifecycle.
OPERATIONAL_ROLES = frozenset({"owner", "admin", "manager", "reception", "technician"})

# Historic role names that were displayed or seeded but never accepted by any
# route. Mapped to their canonical equivalent so an existing row keeps working
# rather than silently losing all access.
LEGACY_ROLE_ALIASES = {
    "location_admin": "admin",
    "location_manager": "manager",
    "accounts": "manager",
    "viewer": "readonly",
}


def normalise_role(role):
    """Return the canonical name for a possibly-legacy role value."""
    value = (role or "").strip().lower()
    return LEGACY_ROLE_ALIASES.get(value, value)


def _is_api_request():
    """True when a 403 should be JSON rather than a redirect to an HTML page."""
    return (
        request.path.startswith(("/api/", "/bookings", "/lifecycle", "/reviews"))
        or request.is_json
        or request.accept_mimetypes.best == "application/json"
    )


def require_role(*allowed):
    """Reject a request whose authenticated user is not in *allowed*.

    Authorisation only -- it deliberately does not re-check authentication or
    tenant scope. Routes keep using login_required/current_location_id for
    those, and PostgreSQL RLS remains the enforcement layer for tenant
    isolation. Platform administrators pass every check.

    Accepts canonical role names; the session value is normalised first so a
    legacy role stored on an existing row still resolves.
    """
    permitted = {normalise_role(r) for r in allowed}

    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            user = session.get("user") or {}
            if not user:
                if _is_api_request():
                    return jsonify({"error": "authentication required"}), 401
                return redirect(url_for("auth.login"))

            role = normalise_role(user.get("role"))
            if role in PLATFORM_ROLES or role in permitted:
                return view(*args, **kwargs)

            if _is_api_request():
                return jsonify({"error": "insufficient permissions"}), 403
            flash("Access denied. You do not have permission to do that.", "error")
            return redirect(url_for("workshop_dashboard.workshop_dashboard"))

        return wrapped

    return decorator


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
