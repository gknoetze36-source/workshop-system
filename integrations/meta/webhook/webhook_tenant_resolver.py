"""Resolve a signed Meta WhatsApp webhook to its PHANTA tenant.

This resolver intentionally runs in the platform read context before normal
tenant RLS is established. Once it returns a tenant id, webhook processing
must switch to tenant_transaction().
"""
from __future__ import annotations

from sqlalchemy import select

from models.integration_models import MetaBusinessConnection


def resolve_meta_webhook_tenant(session, payload: dict) -> int | None:
    for entry in payload.get("entry", []):
        waba_id = str(entry.get("id", "")) or None
        for change in entry.get("changes", []):
            value = change.get("value") or {}
            phone_id = str((value.get("metadata") or {}).get("phone_number_id") or "")
            if phone_id:
                conn = session.scalar(
                    select(MetaBusinessConnection).where(
                        MetaBusinessConnection.phone_number_id == phone_id
                    )
                )
                if conn:
                    return int(conn.tenant_id)
            if waba_id:
                conn = session.scalar(
                    select(MetaBusinessConnection).where(
                        MetaBusinessConnection.waba_id == waba_id
                    )
                )
                if conn:
                    return int(conn.tenant_id)
    return None
