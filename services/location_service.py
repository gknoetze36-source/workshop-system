"""
Location Service for Workshop System Version 2.

This service contains all business logic for the Location entity.
It depends only on the Location Repository.
"""

from repositories.location_repository import get_visible_branches as _get_visible_branches


def get_visible_branches(user=None, franchise_id=None, include_inactive=False, public_only=False):
    """Return a list of visible branches based on user, franchise_id, include_inactive, public_only flags."""
    return _get_visible_branches(user=user, franchise_id=franchise_id, include_inactive=include_inactive, public_only=public_only)
