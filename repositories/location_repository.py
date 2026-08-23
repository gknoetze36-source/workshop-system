"""Canonical Owner -> Location repository.

One owner owns exactly one location. Locations are the sole operational scope.
"""
from database.query import query_db


def get_location_by_id(location_id, owner_id=None):
    clauses=["id=%s"]; params=[location_id]
    if owner_id is not None:
        clauses.append("owner_id=%s"); params.append(owner_id)
    return query_db(f"SELECT * FROM locations WHERE {' AND '.join(clauses)} LIMIT 1", tuple(params), one=True)


def get_visible_locations(user=None, location_id=None, include_inactive=False, public_only=False):
    clauses=[]; params=[]
    if not include_inactive: clauses.append("COALESCE(active, TRUE)=TRUE")
    if public_only: clauses.append("COALESCE(public_booking_enabled, FALSE)=TRUE")
    if user and user.get("role") not in {"super_admin","phanta_admin","platform_admin"}:
        user_location=user.get("location_id")
        if not user_location: return []
        clauses.append("id=%s"); params.append(user_location)
    if location_id is not None:
        clauses.append("id=%s"); params.append(location_id)
    where=(" WHERE "+" AND ".join(clauses)) if clauses else ""
    return query_db(f"SELECT * FROM locations{where} ORDER BY name", tuple(params))


def get_location_for_public_booking(location_slug):
    return query_db("""SELECT * FROM locations
        WHERE slug=%s AND COALESCE(active, TRUE)=TRUE
        AND COALESCE(public_booking_enabled, FALSE)=TRUE LIMIT 1""", (location_slug,), one=True)
