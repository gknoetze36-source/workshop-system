from __future__ import annotations
import secrets
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from sqlalchemy import select
from sqlalchemy.orm import Session
from models.integration_models import MetaSignupSession
from .graph_api_client import GraphApiClient
from ..auth.config import MetaAuthConfig
from ..repositories.connection_repo import MetaConnectionRepository
from ..auth.token_store import MetaTokenStore

@dataclass(frozen=True)
class SignupLaunch:
    state_nonce: str
    expires_at: datetime
    app_id: str
    config_id: str
    graph_api_version: str

@dataclass(frozen=True)
class SignupResult:
    location_id: int
    business_id: str | None
    waba_id: str | None
    phone_number_id: str | None
    token_type: str
    expires_in: int | None
    token_secret_ref: str

class EmbeddedSignupService:
    """Phase 5: launch, callback/session handling, code exchange and persistence.

    Token encryption/expiry is Phase 6; phone registration is Phase 7; webhooks
    are Phase 8. The raw customer token is therefore never stored here.
    """
    SESSION_TTL = timedelta(minutes=15)
    def __init__(self, config=None, client=None, connection_repo=None, token_store=None):
        self.config = config or MetaAuthConfig.from_env()
        self.client = client or GraphApiClient(self.config)
        self.connection_repo = connection_repo or MetaConnectionRepository()
        self.token_store = token_store or MetaTokenStore()

    def begin(self, session: Session, location_id: int) -> SignupLaunch:
        if not isinstance(location_id, int) or location_id <= 0:
            raise ValueError("location_id must be a positive integer")
        nonce = secrets.token_urlsafe(32)
        expires = datetime.now(timezone.utc) + self.SESSION_TTL
        session.add(MetaSignupSession(location_id=location_id, state_nonce=nonce, expires_at=expires, status="started"))
        session.flush()
        return SignupLaunch(nonce, expires, self.config.app_id, self.config.embedded_signup_config_id, self.config.graph_api_version)

    def complete(self, session: Session, *, location_id: int, state_nonce: str, code: str, business_id=None, waba_id=None, phone_number_id=None) -> SignupResult:
        now = datetime.now(timezone.utc)
        signup = session.scalar(select(MetaSignupSession).where(MetaSignupSession.location_id == location_id, MetaSignupSession.state_nonce == state_nonce))
        if signup is None:
            raise ValueError("Unknown Embedded Signup session")
        if signup.status != "started" or signup.consumed_at is not None:
            raise ValueError("Embedded Signup session has already been consumed")
        expires_at = signup.expires_at if signup.expires_at.tzinfo is not None else signup.expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= now:
            signup.status = "expired"
            session.flush()
            raise ValueError("Embedded Signup session has expired")
        if not code or len(code) > 4096:
            raise ValueError("Embedded Signup authorization code is required")
        if not waba_id or not phone_number_id:
            raise ValueError("WhatsApp Embedded Signup did not return a WhatsApp Business Account and phone number. Please complete signup again.")
        token_payload = self.client.exchange_embedded_signup_code(code)
        token = token_payload["access_token"]
        signup.business_id = business_id or signup.business_id
        signup.waba_id = waba_id or signup.waba_id
        signup.phone_number_id = phone_number_id or signup.phone_number_id
        signup.status = "completed"
        signup.consumed_at = now
        connection = self.connection_repo.upsert_connection(session, location_id, business_id=signup.business_id, waba_id=signup.waba_id, phone_number_id=signup.phone_number_id, token_type="business_integration_system_user", connection_status="connected", connected_at=now)
        expires_in = token_payload.get("expires_in")
        expires_at = now + timedelta(seconds=int(expires_in)) if expires_in else None
        self.token_store.save_customer_token(session, connection, token, expires_at=expires_at)
        session.flush()
        return SignupResult(location_id, connection.business_id, connection.waba_id, connection.phone_number_id, connection.token_type, expires_in, connection.token_secret_ref or "")
