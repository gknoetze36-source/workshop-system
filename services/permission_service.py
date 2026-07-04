"""
Permission Service for Workshop System Version 2.

This service contains all permission logic.
It depends only on the Permission Repository.
"""

from repositories.permission_repository import (
    get_allowed_roles_for_creator,
    can_user_create_role,
    get_all_roles,
)


def get_allowed_roles_for_creator(user):
    """Return list of roles that the user is allowed to create."""
    return get_allowed_roles_for_creator(user)


def can_user_create_role(user, role):
    """Check if the user is allowed to create the given role."""
    return can_user_create_role(user, role)


def get_all_roles():
    """Return list of all possible roles."""
    return get_all_roles()
