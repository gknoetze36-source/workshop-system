"""Store and read an InstagramConnection's token, encrypted with the same
Fernet key as every other Meta token (META_TOKEN_ENCRYPTION_KEY)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from integrations.meta.auth.token_store import MetaTokenStore


def save_token(connection, payload: dict) -> None:
    """payload is Meta's {access_token, expires_in} long-lived token response."""
    now = datetime.now(timezone.utc)
    connection.encrypted_access_token = MetaTokenStore().encrypt(payload["access_token"])
    expires_in = payload.get("expires_in")
    connection.token_expires_at = now + timedelta(seconds=int(expires_in)) if expires_in else None
    connection.token_refreshed_at = now
    connection.connection_status = "connected"


def get_token(connection) -> str:
    return MetaTokenStore().decrypt(connection.encrypted_access_token)
