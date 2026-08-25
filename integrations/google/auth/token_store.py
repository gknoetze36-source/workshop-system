"""Encrypted Google Business Profile refresh token storage.

Deliberately reuses META_TOKEN_ENCRYPTION_KEY rather than introducing a
third encryption key env var alongside it and
PAYSTACK_AUTH_ENCRYPTION_KEY -- one key, one thing to keep durable and
never rotate carelessly (see the deployment guide's warning about this).
The Fernet cipher itself doesn't care what it's encrypting; this class
exists to keep the Google-specific save/get methods separate from
integrations/meta/auth/token_store.py's Meta-specific ones, not because
the encryption itself needs to differ.
"""
from __future__ import annotations

import os
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy.orm import Session


class GoogleTokenStore:
    ENV_KEY = "META_TOKEN_ENCRYPTION_KEY"

    def __init__(self, key: str | bytes | None = None):
        raw = key if key is not None else os.getenv(self.ENV_KEY, "").strip()
        if isinstance(raw, str):
            raw = raw.encode()
        if not raw:
            raise RuntimeError(f"{self.ENV_KEY} is required for Google token storage")
        try:
            self._fernet = Fernet(raw)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"{self.ENV_KEY} must be a valid Fernet key; generate one with Fernet.generate_key()"
            ) from exc

    def encrypt(self, token: str) -> str:
        if not token or not isinstance(token, str):
            raise ValueError("Google refresh token must be a non-empty string")
        return self._fernet.encrypt(token.encode()).decode()

    def decrypt(self, ciphertext: str) -> str:
        if not ciphertext:
            raise ValueError("Encrypted Google refresh token is missing")
        try:
            return self._fernet.decrypt(ciphertext.encode()).decode()
        except InvalidToken as exc:
            raise ValueError("Unable to decrypt Google refresh token with the configured key") from exc

    def save_refresh_token(self, session: Session, connection, refresh_token: str):
        connection.encrypted_refresh_token = self.encrypt(refresh_token)
        connection.token_key_version = "v1"
        connection.connection_status = "connected"
        session.flush()
        return connection

    def get_refresh_token(self, connection) -> str:
        return self.decrypt(connection.encrypted_refresh_token)
