"""Resolve a signed Meta WhatsApp webhook to exactly one PHANTA location."""
from __future__ import annotations
from sqlalchemy import select
from models.integration_models import MetaBusinessConnection


def resolve_meta_webhook_location(session, payload: dict) -> int | None:
    """Return one location only when every identifiable asset maps to it.

    A webhook that contains assets belonging to different locations is rejected
    instead of allowing the first matching asset to select the transaction scope.
    """
    resolved: set[int] = set()
    for entry in payload.get("entry", []):
        waba_id = str(entry.get("id", "")) or None
        for change in entry.get("changes", []):
            value = change.get("value") or {}
            phone_id = str((value.get("metadata") or {}).get("phone_number_id") or "")
            conn = None
            if phone_id:
                conn = session.scalar(select(MetaBusinessConnection).where(MetaBusinessConnection.phone_number_id == phone_id))
            if conn is None and waba_id:
                conn = session.scalar(select(MetaBusinessConnection).where(MetaBusinessConnection.waba_id == waba_id))
            if conn is not None:
                resolved.add(int(conn.location_id))
    if len(resolved) != 1:
        return None
    return next(iter(resolved))
