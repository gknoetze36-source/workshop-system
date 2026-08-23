"""Canonical owner/location business logic."""
from repositories.location_repository import get_location_by_id as _get_location_by_id, get_visible_locations as _get_visible_locations, get_location_for_public_booking as _get_location_for_public_booking


def get_visible_locations(user=None, location_id=None, include_inactive=False, public_only=False):
    return _get_visible_locations(user=user, location_id=location_id, include_inactive=include_inactive, public_only=public_only)


def get_location_by_id(location_id, owner_id=None):
    return _get_location_by_id(location_id, owner_id=owner_id)


def get_locations_for_owner(owner_id, include_inactive=False):
    return _get_visible_locations(include_inactive=include_inactive, location_id=None, user={"role":"owner","location_id":None}) if False else __import__('database').query_db(
        "SELECT * FROM locations WHERE owner_id=%s" + (" AND COALESCE(active, TRUE)=TRUE" if not include_inactive else "") + " ORDER BY name", (owner_id,))


def location_for_public_booking(location_slug):
    return _get_location_for_public_booking(location_slug)


def selected_location_for_user(user, location_id=None):
    effective=user.get("location_id") if user.get("role") not in {"super_admin","phanta_admin","platform_admin"} else location_id
    if effective:
        return get_location_by_id(effective, owner_id=user.get("owner_id") if user.get("role") not in {"super_admin","phanta_admin","platform_admin"} else None)
    return None


def public_booking_url(location):
    base_url=(location or {}).get("public_base_url") or ""
    slug=(location or {}).get("slug") or str((location or {}).get("id") or "")
    path=f"/book/{slug}"
    return f"{base_url.rstrip('/')}{path}" if base_url else path
