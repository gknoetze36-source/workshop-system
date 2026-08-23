"""Canonical Meta permissions required by PHANTA v1."""
from __future__ import annotations

from .config import REQUIRED_META_PERMISSIONS


def validate_permissions(granted: set[str] | list[str] | tuple[str, ...]) -> dict[str, bool]:
    granted_set = set(granted)
    return {
        permission: permission in granted_set
        for permission in REQUIRED_META_PERMISSIONS
    }


def missing_permissions(granted: set[str] | list[str] | tuple[str, ...]) -> list[str]:
    return sorted(set(REQUIRED_META_PERMISSIONS) - set(granted))


def all_required_permissions_present(
    granted: set[str] | list[str] | tuple[str, ...],
) -> bool:
    return not missing_permissions(granted)
