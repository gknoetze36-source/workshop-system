"""Canonical authenticated owner -> location context."""
from flask import g, session


def current_location_id() -> int:
    location_id=getattr(g,"location_id",None) or (session.get("user") or {}).get("location_id")
    if not isinstance(location_id,int) or location_id<=0:
        raise PermissionError("authenticated owner/location context is required")
    return location_id
