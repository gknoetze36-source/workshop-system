"""
Owner Service for Workshop System Version 2.

This service contains all business logic for the Owner entity.
It depends only on the Owner Repository.
"""

from repositories.owner_repository import get_visible_owners as repo_get_visible_owners


def get_owner_by_id(owner_id):
    """Retrieve an owner by their ID."""
    raise NotImplementedError("TODO: Implement get_owner_by_id")


def get_owner_by_slug(slug):
    """Retrieve an owner by their slug."""
    raise NotImplementedError("TODO: Implement get_owner_by_slug")


def list_owners(limit=None, offset=None):
    """List owners with optional pagination."""
    raise NotImplementedError("TODO: Implement list_owners")


def create_owner(owner_data):
    """Create a new owner. Returns the new owner's ID."""
    raise NotImplementedError("TODO: Implement create_owner")


def update_owner(owner_id, owner_data):
    """Update an existing owner."""
    raise NotImplementedError("TODO: Implement update_owner")


def delete_owner(owner_id):
    """Delete an owner by ID."""
    raise NotImplementedError("TODO: Implement delete_owner")


def get_visible_owners(user=None, include_inactive=False):
    """
    Return a list of visible owners (franchises) based on user and include_inactive flag.
    """
    return repo_get_visible_owners(user=user, include_inactive=include_inactive)
