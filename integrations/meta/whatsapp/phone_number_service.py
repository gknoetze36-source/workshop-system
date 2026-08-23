"""Phase 7 Meta WhatsApp phone-number registration and verification."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from models.integration_models import MetaBusinessConnection, MetaBusinessVerificationStatus
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.repositories.connection_repo import MetaConnectionRepository
from integrations.meta.services.graph_api_client import GraphApiClient

_PIN_RE = re.compile(r"^\d{6}$")
_CODE_RE = re.compile(r"^\d{4,8}$")


class PhoneRegistrationError(ValueError):
    pass


@dataclass(frozen=True)
class PhoneRegistrationResult:
    location_id: int
    phone_number_id: str
    status: str
    success: bool
    message: str | None = None


class PhoneNumberService:
    """Owns the Phase 7 phone registration workflow.

    The six-digit PIN and verification codes are request-only secrets. They
    are validated and sent to Meta, then discarded; they are never persisted,
    returned, or logged by PHANTA.
    """

    def __init__(self, config=None, client=None, token_store=None, connection_repo=None):
        if client is None:
            from integrations.meta.auth.config import MetaAuthConfig
            config = config or MetaAuthConfig.from_env()
            client = GraphApiClient(config)
        self.client = client
        self.token_store = token_store or MetaTokenStore()
        self.connection_repo = connection_repo or MetaConnectionRepository()

    def _connection(self, session: Session, location_id: int) -> MetaBusinessConnection:
        connection = self.connection_repo.get_for_location(session, location_id)
        if connection is None:
            raise PhoneRegistrationError("Meta WhatsApp is not connected for this workshop")
        if not connection.waba_id or not connection.phone_number_id:
            raise PhoneRegistrationError("Meta WABA and phone number details are incomplete")
        if not connection.encrypted_access_token:
            raise PhoneRegistrationError("Meta customer access token is unavailable; reconnect WhatsApp")
        return connection

    def _token(self, connection: MetaBusinessConnection) -> str:
        try:
            return self.token_store.get_customer_token(connection)
        except ValueError as exc:
            raise PhoneRegistrationError("Meta customer access token cannot be decrypted; reconnect WhatsApp") from exc

    @staticmethod
    def _validate_pin(pin: str) -> str:
        if not isinstance(pin, str) or not _PIN_RE.fullmatch(pin):
            raise PhoneRegistrationError("PIN must be exactly 6 digits")
        return pin

    @staticmethod
    def _validate_code(code: str) -> str:
        if not isinstance(code, str) or not _CODE_RE.fullmatch(code):
            raise PhoneRegistrationError("Verification code must contain 4 to 8 digits")
        return code

    def register(self, session: Session, location_id: int, pin: str) -> PhoneRegistrationResult:
        connection = self._connection(session, location_id)
        pin = self._validate_pin(pin)
        token = self._token(connection)
        payload = self.client.post_with_token(
            token,
            f"/{connection.phone_number_id}/register",
            data={"messaging_product": "whatsapp", "pin": pin},
        )
        if payload.get("success") is not True:
            raise PhoneRegistrationError("Meta did not confirm phone number registration")
        self._set_phone_status(session, connection, "registered")
        return PhoneRegistrationResult(location_id, connection.phone_number_id, "registered", True)

    def request_verification_code(self, session: Session, location_id: int, *, code_method: str, language: str) -> PhoneRegistrationResult:
        method = str(code_method).upper()
        if method not in {"SMS", "VOICE"}:
            raise PhoneRegistrationError("code_method must be SMS or VOICE")
        if not language or len(language) > 20:
            raise PhoneRegistrationError("language is required")
        connection = self._connection(session, location_id)
        token = self._token(connection)
        payload = self.client.post_with_token(
            token,
            f"/{connection.phone_number_id}/request_code",
            data={"code_method": method, "language": language},
        )
        if payload.get("success") is not True:
            raise PhoneRegistrationError("Meta did not confirm the verification-code request")
        self._set_phone_status(session, connection, "code_requested")
        return PhoneRegistrationResult(location_id, connection.phone_number_id, "code_requested", True)

    def verify_code(self, session: Session, location_id: int, code: str) -> PhoneRegistrationResult:
        connection = self._connection(session, location_id)
        code = self._validate_code(code)
        token = self._token(connection)
        payload = self.client.post_with_token(
            token,
            f"/{connection.phone_number_id}/verify_code",
            data={"code": code},
        )
        if payload.get("success") is not True:
            raise PhoneRegistrationError("Meta did not confirm phone number verification")
        self._set_phone_status(session, connection, "verified")
        return PhoneRegistrationResult(location_id, connection.phone_number_id, "verified", True)

    def set_pin(self, session: Session, location_id: int, pin: str) -> PhoneRegistrationResult:
        """Set/update the two-step verification PIN.

        Meta does not expose a disable endpoint; this operation is deliberately
        named set_pin to make that limitation explicit.
        """
        connection = self._connection(session, location_id)
        pin = self._validate_pin(pin)
        token = self._token(connection)
        payload = self.client.post_with_token(token, f"/{connection.phone_number_id}", data={"pin": pin})
        if payload.get("success") is not True:
            raise PhoneRegistrationError("Meta did not confirm the two-step verification PIN update")
        return PhoneRegistrationResult(location_id, connection.phone_number_id, "pin_set", True)

    def phone_info(self, session: Session, location_id: int) -> dict[str, Any]:
        connection = self._connection(session, location_id)
        token = self._token(connection)
        payload = self.client.get_with_token(
            token,
            f"/{connection.phone_number_id}",
            params={"fields": "display_phone_number,verified_name,quality_rating"},
        )
        self._apply_phone_info(connection, payload)
        self._set_phone_status(session, connection, self._current_phone_status(session, connection) or "registered")
        return {
            "phone_number_id": connection.phone_number_id,
            "display_phone_number": payload.get("display_phone_number"),
            "verified_name": payload.get("verified_name"),
            "quality_rating": payload.get("quality_rating"),
            "phone_verification_status": self._current_phone_status(session, connection),
        }

    def waba_info(self, session: Session, location_id: int) -> dict[str, Any]:
        connection = self._connection(session, location_id)
        token = self._token(connection)
        payload = self.client.get_with_token(
            token,
            f"/{connection.waba_id}",
            params={"fields": "name,timezone_id,message_template_namespace"},
        )
        return {
            "waba_id": connection.waba_id,
            "name": payload.get("name"),
            "timezone_id": payload.get("timezone_id"),
            "message_template_namespace": payload.get("message_template_namespace"),
        }

    def sync_phone_status(self, session: Session, location_id: int) -> dict[str, Any]:
        """Read Meta's current phone display state and persist safe status data."""
        return self.phone_info(session, location_id)

    @staticmethod
    def _apply_phone_info(connection: MetaBusinessConnection, payload: dict[str, Any]) -> None:
        if payload.get("display_phone_number") is not None:
            connection.display_phone_number = str(payload["display_phone_number"])
        if payload.get("verified_name") is not None:
            connection.verified_name = str(payload["verified_name"])
        if payload.get("quality_rating") is not None:
            connection.quality_rating = str(payload["quality_rating"])

    @staticmethod
    def _set_phone_status(session: Session, connection: MetaBusinessConnection, status: str) -> None:
        row = session.scalar(
            select(MetaBusinessVerificationStatus).where(
                MetaBusinessVerificationStatus.location_id == connection.location_id,
                MetaBusinessVerificationStatus.connection_id == connection.id,
            )
        )
        now = datetime.now(timezone.utc)
        if row is None:
            row = MetaBusinessVerificationStatus(
                location_id=connection.location_id,
                connection_id=connection.id,
            )
            session.add(row)
        row.phone_verification_status = status
        row.last_checked_at = now
        session.flush()

    @staticmethod
    def _current_phone_status(session: Session, connection: MetaBusinessConnection) -> str | None:
        row = session.scalar(
            select(MetaBusinessVerificationStatus).where(
                MetaBusinessVerificationStatus.location_id == connection.location_id,
                MetaBusinessVerificationStatus.connection_id == connection.id,
            )
        )
        return row.phone_verification_status if row else None
