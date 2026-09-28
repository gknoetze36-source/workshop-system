"""Encrypted TikTok token storage.

Mirrors XTokenStore's shape: two encrypted tokens (access + refresh),
since TikTok, like X, issues a separate refresh_token distinct from the
access token -- unlike Threads, which self-refreshes a single
long-lived token in place.

save_connection_tokens() exists specifically to support refresh token
rotation: TikTok's own docs state plainly that "the returned
refresh_token may be different than the one passed in the payload...
you must use the newly-returned token" (developers.tiktok.com/doc/
oauth-user-access-token-management). Every refresh MUST persist
whatever refresh_token comes back, every time --
this method makes that the only way to save tokens after a refresh, so
there is no code path that saves a new access_token while leaving a
stale refresh_token in place.
"""
from __future__ import annotations

import os
from datetime import datetime

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy.orm import Session

from models.integration_models import TikTokConnection


class TikTokTokenStore:
    ENV_KEY = "TIKTOK_TOKEN_ENCRYPTION_KEY"
    KEY_VERSION = "v1"

    def __init__(self, key: str | bytes | None = None):
        raw = key if key is not None else os.getenv(self.ENV_KEY, "").strip()
        if isinstance(raw, str):
            raw = raw.encode()
        if not raw:
            raise RuntimeError(f"{self.ENV_KEY} is required for TikTok token storage")
        try:
            self._fernet = Fernet(raw)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"{self.ENV_KEY} must be a valid Fernet key; generate one with Fernet.generate_key()"
            ) from exc

    @classmethod
    def generate_key(cls) -> str:
        return Fernet.generate_key().decode()

    def encrypt(self, token: str) -> str:
        if not token or not isinstance(token, str):
            raise ValueError("TikTok token must be a non-empty string")
        return self._fernet.encrypt(token.encode()).decode()

    def decrypt(self, ciphertext: str) -> str:
        if not ciphertext:
            raise ValueError("Encrypted TikTok token is missing")
        try:
            return self._fernet.decrypt(ciphertext.encode()).decode()
        except InvalidToken as exc:
            raise ValueError("Unable to decrypt TikTok token with the configured key") from exc

    def save_connection_tokens(
        self, session: Session, connection: TikTokConnection, *,
        access_token: str, refresh_token: str, expires_at: datetime | None = None,
    ) -> TikTokConnection:
        """Both tokens are required arguments, not optional -- unlike
        XTokenStore.save_connection_tokens()'s refresh_token=None
        default (X does not always rotate it). TikTok's refresh
        response always includes a refresh_token, whether or not its
        value actually changed, so there is no legitimate case here
        where only the access token should be saved."""
        connection.encrypted_access_token = self.encrypt(access_token)
        connection.encrypted_refresh_token = self.encrypt(refresh_token)
        connection.token_expires_at = expires_at
        connection.token_key_version = self.KEY_VERSION
        connection.connection_status = "connected"
        session.flush()
        return connection

    def get_access_token(self, connection: TikTokConnection) -> str:
        return self.decrypt(connection.encrypted_access_token)

    def get_refresh_token(self, connection: TikTokConnection) -> str:
        return self.decrypt(connection.encrypted_refresh_token)
