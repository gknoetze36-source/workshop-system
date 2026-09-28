"""Encrypted Threads token storage.

Mirrors MetaTokenStore/XTokenStore's shape exactly -- same Fernet
encrypt/decrypt, same key-from-environment pattern. A separate
environment variable (THREADS_TOKEN_ENCRYPTION_KEY), consistent with
this codebase's one-key-per-platform-integration convention.

Only one token to store, not two: Threads' long-lived user access
token IS the refreshable credential (GET /refresh_access_token extends
it in place, see integrations/threads/api_client.py) -- there is no
separate refresh_token the way X's OAuth 2.0 grant issues one.
"""
from __future__ import annotations

import os
from datetime import datetime

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy.orm import Session

from models.integration_models import ThreadsConnection


class ThreadsTokenStore:
    ENV_KEY = "THREADS_TOKEN_ENCRYPTION_KEY"
    KEY_VERSION = "v1"

    def __init__(self, key: str | bytes | None = None):
        raw = key if key is not None else os.getenv(self.ENV_KEY, "").strip()
        if isinstance(raw, str):
            raw = raw.encode()
        if not raw:
            raise RuntimeError(f"{self.ENV_KEY} is required for Threads token storage")
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
            raise ValueError("Threads token must be a non-empty string")
        return self._fernet.encrypt(token.encode()).decode()

    def decrypt(self, ciphertext: str) -> str:
        if not ciphertext:
            raise ValueError("Encrypted Threads token is missing")
        try:
            return self._fernet.decrypt(ciphertext.encode()).decode()
        except InvalidToken as exc:
            raise ValueError("Unable to decrypt Threads token with the configured key") from exc

    def save_long_lived_token(
        self, session: Session, connection: ThreadsConnection, *, token: str, expires_at: datetime | None = None,
    ) -> ThreadsConnection:
        connection.encrypted_long_lived_token = self.encrypt(token)
        connection.token_expires_at = expires_at
        connection.token_key_version = self.KEY_VERSION
        connection.connection_status = "connected"
        session.flush()
        return connection

    def get_long_lived_token(self, connection: ThreadsConnection) -> str:
        return self.decrypt(connection.encrypted_long_lived_token)
