"""Encrypted X (Twitter) token storage.

Mirrors integrations/meta/auth/token_store.py's MetaTokenStore exactly
-- same Fernet-based encrypt/decrypt, same key-from-environment
pattern, same shape. A separate environment variable
(X_TOKEN_ENCRYPTION_KEY) rather than reusing META_TOKEN_ENCRYPTION_KEY:
X's key should be independently rotatable from Meta's, consistent with
this codebase's existing convention of one encryption key per platform
integration (Meta and Google already each have their own).
"""
from __future__ import annotations

import os
from datetime import datetime

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy.orm import Session

from models.integration_models import XConnection


class XTokenStore:
    ENV_KEY = "X_TOKEN_ENCRYPTION_KEY"
    KEY_VERSION = "v1"

    def __init__(self, key: str | bytes | None = None):
        raw = key if key is not None else os.getenv(self.ENV_KEY, "").strip()
        if isinstance(raw, str):
            raw = raw.encode()
        if not raw:
            raise RuntimeError(f"{self.ENV_KEY} is required for X token storage")
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
            raise ValueError("X token must be a non-empty string")
        return self._fernet.encrypt(token.encode()).decode()

    def decrypt(self, ciphertext: str) -> str:
        if not ciphertext:
            raise ValueError("Encrypted X token is missing")
        try:
            return self._fernet.decrypt(ciphertext.encode()).decode()
        except InvalidToken as exc:
            raise ValueError("Unable to decrypt X token with the configured key") from exc

    def save_connection_tokens(
        self, session: Session, connection: XConnection, *, access_token: str,
        refresh_token: str | None = None, expires_at: datetime | None = None,
    ) -> XConnection:
        connection.encrypted_access_token = self.encrypt(access_token)
        if refresh_token is not None:
            connection.encrypted_refresh_token = self.encrypt(refresh_token)
        connection.token_expires_at = expires_at
        connection.token_key_version = self.KEY_VERSION
        connection.connection_status = "connected"
        session.flush()
        return connection

    def get_access_token(self, connection: XConnection) -> str:
        return self.decrypt(connection.encrypted_access_token)

    def get_refresh_token(self, connection: XConnection) -> str | None:
        if not connection.encrypted_refresh_token:
            return None
        return self.decrypt(connection.encrypted_refresh_token)
