"""Encrypted customer Meta token storage for Phase 6."""
from __future__ import annotations

import base64
import os
from datetime import datetime

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy.orm import Session

from models.integration_models import MetaBusinessConnection


class MetaTokenStore:
    """Encrypts customer-scoped Meta business tokens before database storage.

    The encryption key is supplied through META_TOKEN_ENCRYPTION_KEY and is
    never persisted in the database. A Fernet key is 32 urlsafe-base64 bytes.
    """

    ENV_KEY = "META_TOKEN_ENCRYPTION_KEY"
    KEY_VERSION = "v1"

    def __init__(self, key: str | bytes | None = None):
        raw = key if key is not None else os.getenv(self.ENV_KEY, "").strip()
        if isinstance(raw, str):
            raw = raw.encode()
        if not raw:
            raise RuntimeError(f"{self.ENV_KEY} is required for Meta token storage")
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
            raise ValueError("Meta access token must be a non-empty string")
        return self._fernet.encrypt(token.encode()).decode()

    def decrypt(self, ciphertext: str) -> str:
        if not ciphertext:
            raise ValueError("Encrypted Meta token is missing")
        try:
            return self._fernet.decrypt(ciphertext.encode()).decode()
        except InvalidToken as exc:
            raise ValueError("Unable to decrypt Meta access token with the configured key") from exc

    def save_customer_token(
        self,
        session: Session,
        connection: MetaBusinessConnection,
        token: str,
        *,
        expires_at: datetime | None = None,
    ) -> MetaBusinessConnection:
        connection.encrypted_access_token = self.encrypt(token)
        connection.token_key_version = self.KEY_VERSION
        connection.token_secret_ref = f"meta-token:{connection.location_id}:{connection.id}"
        connection.token_expires_at = expires_at
        connection.connection_status = "connected"
        session.flush()
        return connection

    def get_customer_token(self, connection: MetaBusinessConnection) -> str:
        return self.decrypt(connection.encrypted_access_token)

    def save_social_token(self, session: Session, connection, token: str, *, expires_at: datetime | None = None):
        connection.encrypted_page_access_token = self.encrypt(token)
        connection.token_key_version = self.KEY_VERSION
        connection.token_expires_at = expires_at
        connection.connection_status = "connected"
        session.flush()
        return connection

    def get_social_token(self, connection) -> str:
        return self.decrypt(connection.encrypted_page_access_token)


    def save_social_oauth_token(self, session: Session, oauth_session, token: str):
        oauth_session.encrypted_user_access_token = self.encrypt(token)
        session.flush()
        return oauth_session

    def get_social_oauth_token(self, oauth_session) -> str:
        return self.decrypt(oauth_session.encrypted_user_access_token)
