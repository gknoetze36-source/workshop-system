"""Meta customer-token validation, expiry and reconnect state."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from models.integration_models import MetaBusinessConnection
from ..auth.token_store import MetaTokenStore
from ..repositories.connection_repo import MetaConnectionRepository
from .graph_api_client import GraphApiClient, MetaGraphAPIError


@dataclass(frozen=True)
class MetaConnectionHealth:
    location_id: int
    connection_id: int | None
    status: str
    healthy: bool
    reconnect_required: bool
    token_valid: bool | None
    expires_at: datetime | None
    expires_in_seconds: int | None
    permissions: tuple[str, ...]
    granular_scopes: tuple[dict[str, Any], ...]
    checked_at: datetime
    error: str | None = None


class MetaTokenStatusService:
    EXPIRY_WARNING = timedelta(days=7)

    def __init__(self, config=None, client=None, token_store=None, connection_repo=None):
        from ..auth.config import MetaAuthConfig

        self.config = config or MetaAuthConfig.from_env()
        self.client = client or GraphApiClient(self.config)
        self.token_store = token_store or MetaTokenStore()
        self.connection_repo = connection_repo or MetaConnectionRepository()

    @staticmethod
    def _expires_at_from_debug(payload: dict[str, Any]) -> datetime | None:
        expires_at = payload.get("data", {}).get("expires_at")
        if not expires_at:
            return None
        return datetime.fromtimestamp(int(expires_at), tz=timezone.utc)

    def check_connection(self, session: Session, location_id: int) -> MetaConnectionHealth:
        connection = self.connection_repo.get_for_location(session, location_id)
        checked_at = datetime.now(timezone.utc)
        if connection is None:
            return MetaConnectionHealth(
                location_id, None, "not_connected", False, True, None, None, None,
                (), (), checked_at, "No Meta connection exists",
            )

        if not connection.encrypted_access_token:
            connection.connection_status = "reconnect_required"
            connection.last_health_check_at = checked_at
            session.flush()
            return MetaConnectionHealth(
                location_id, connection.id, "reconnect_required", False, True, None,
                connection.token_expires_at, None, (), (), checked_at,
                "Meta customer access token is not stored",
            )

        try:
            token = self.token_store.get_customer_token(connection)
            payload = self.client.debug_customer_token(token)
            data = payload.get("data", {})
            valid = bool(data.get("is_valid"))
            expires_at = self._expires_at_from_debug(payload)
            if expires_at is not None:
                connection.token_expires_at = expires_at
            connection.last_health_check_at = checked_at

            granular = tuple(data.get("granular_scopes") or ())
            permissions = tuple(sorted({
                str(item.get("scope")) for item in granular
                if isinstance(item, dict) and item.get("scope")
            }))

            if not valid:
                connection.connection_status = "reconnect_required"
                session.flush()
                return MetaConnectionHealth(
                    location_id, connection.id, "reconnect_required", False, True, False,
                    expires_at, self._seconds_until(expires_at, checked_at), permissions,
                    granular, checked_at, "Meta customer token is invalid",
                )

            if expires_at is not None and expires_at <= checked_at:
                connection.connection_status = "reconnect_required"
                session.flush()
                return MetaConnectionHealth(
                    location_id, connection.id, "reconnect_required", False, True, False,
                    expires_at, 0, permissions, granular, checked_at,
                    "Meta customer token has expired",
                )

            status = "expiring_soon" if expires_at and expires_at - checked_at <= self.EXPIRY_WARNING else "connected"
            connection.connection_status = status
            session.flush()
            return MetaConnectionHealth(
                location_id, connection.id, status, True, False, True, expires_at,
                self._seconds_until(expires_at, checked_at), permissions, granular,
                checked_at,
            )
        except (ValueError, MetaGraphAPIError) as exc:
            connection.connection_status = "reconnect_required"
            connection.last_health_check_at = checked_at
            session.flush()
            return MetaConnectionHealth(
                location_id, connection.id, "reconnect_required", False, True, None,
                connection.token_expires_at,
                self._seconds_until(connection.token_expires_at, checked_at),
                (), (), checked_at, str(exc),
            )

    @staticmethod
    def _seconds_until(expires_at: datetime | None, now: datetime) -> int | None:
        if expires_at is None:
            return None
        return max(0, int((expires_at - now).total_seconds()))

    def monitor_location(self, session: Session, location_id: int) -> MetaConnectionHealth:
        return self.check_connection(session, location_id)
