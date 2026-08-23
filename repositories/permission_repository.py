"""
Permission Repository for Workshop System Version 2.

This repository handles all permission logic.
It imports the canonical permission and common helpers directly.
"""

from helpers.permission import available_roles_for_creator
from helpers.common import ROLE_LABELS


def get_allowed_roles_for_creator(user):
    """Return list of roles that the user is allowed to create."""
    return available_roles_for_creator(user)


def can_user_create_role(user, role):
    """Check if the user is allowed to create the given role."""
    allowed = get_allowed_roles_for_creator(user)
    return role in allowed


def get_all_roles():
    """Return list of all possible roles."""
    return list(ROLE_LABELS.keys())
