"""Legacy module path retained only to avoid import breakage.

All active ownership operations use repositories.location_repository and the
canonical owner -> location hierarchy. This shim contains no legacy database
access.
"""
from repositories.location_repository import get_location_by_id, get_visible_locations


def get_owner_location_by_id(location_id):
    return get_location_by_id(location_id)


def get_visible_owner_locations(user=None):
    return get_visible_locations(user=user)
