import json

from database import execute_db, query_db, utc_now
from .common import fetch_all

def record_audit(action, entity_type, entity_id=None, actor_user=None, location_id=None, user_id=None, details=None, before=None, after=None):
    """Write one audit record.

    ``audit_logs`` physically carries two historically separate column sets:
    the raw set used here (actor_user_id / details_json) and the ORM set used
    by repositories/audit_repo.py (actor / before / after), which
    database/compatibility.py adds to the table at boot. Neither writer used
    to populate the other's columns, so rows written through the ORM appeared
    with no actor in fetch_audit_logs() -- the only place audit records are
    actually read.

    This function now fills both representations so a single reader can render
    every row regardless of which code path produced it. ``actor`` is the
    human-readable identity (username/email); ``actor_user_id`` remains the
    foreign key. before/after are optional structured state, matching the ORM
    writer's shape.
    """
    actor_user = actor_user or {}
    actor_label = (
        actor_user.get("username")
        or actor_user.get("email")
        or ("system" if actor_user.get("id") is None else str(actor_user.get("id")))
    )
    execute_db(
        """
        INSERT INTO audit_logs (
            location_id, user_id, actor_user_id, actor, action,
            entity_type, entity_id, details_json, "before", "after", created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            location_id if location_id is not None else actor_user.get("location_id"),
            user_id,
            actor_user.get("id"),
            actor_label,
            action,
            entity_type,
            str(entity_id or ""),
            json.dumps(details or {}, separators=(",", ":"), sort_keys=True),
            json.dumps(before, separators=(",", ":"), sort_keys=True) if before is not None else None,
            json.dumps(after, separators=(",", ":"), sort_keys=True) if after is not None else None,
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
        SELECT al.*,
               l.name AS location_name,
               COALESCE(u.username, al.actor) AS actor_username
        FROM audit_logs al
        LEFT JOIN locations l ON l.id = al.location_id
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

