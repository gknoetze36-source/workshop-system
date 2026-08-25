"""Shared authenticated tenant-context helper for Flask routes."""
from flask import g, session


def current_tenant_id() -> int:
    tenant_id = getattr(g, "tenant_id", None) or (session.get("user") or {}).get("franchise_id")
    if not isinstance(tenant_id, int) or tenant_id <= 0:
        raise PermissionError("authenticated tenant context is required")
    return tenant_id
