import json

from database import execute_db, query_db, utc_now
from .common import fetch_all

def record_audit(action, entity_type, entity_id=None, actor_user=None, location_id=None, user_id=None, details=None):
    actor_user = actor_user or {}
    execute_db(
        """
        INSERT INTO audit_logs (
            location_id, user_id, actor_user_id, action,
            entity_type, entity_id, details_json, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            location_id if location_id is not None else actor_user.get("location_id"),
            user_id,
            actor_user.get("id"),
            action,
            entity_type,
            str(entity_id or ""),
            json.dumps(details or {}, separators=(",", ":"), sort_keys=True),
            utc_now(),
        ),
    )

def fetch_audit_logs(user=None, location_id=None, limit=100):
    clauses = []
    args = []
    if user and user.get("role") != "super_admin":
        clauses.append("al.location_id=%s")
        args.append(user.get("location_id"))
    elif location_id:
        clauses.append("al.location_id=%s")
        args.append(location_id)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    args.append(limit)
    results = query_db(
        f"""
        SELECT al.*, f.name AS location_name, b.name AS location_name, u.username AS actor_username
        FROM audit_logs al
        LEFT JOIN locations f ON f.id = al.location_id
        LEFT JOIN locations b ON b.id = al.location_id
        LEFT JOIN users u ON u.id = al.actor_user_id
        {where}
        ORDER BY al.created_at DESC
        LIMIT %s
        """,
        tuple(args),
    )
    return results or []

def fetch_credential_audit():
    return fetch_all(
        """
        SELECT ca.*, u.full_name AS actor_name, f.name AS location_name
        FROM credential_audit ca
        LEFT JOIN users u ON u.id = ca.actor_user_id
        LEFT JOIN locations f ON f.id = ca.location_id
        ORDER BY ca.created_at DESC
        """
    )

