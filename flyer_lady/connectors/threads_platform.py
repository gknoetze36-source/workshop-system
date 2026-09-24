"""Threads connector.

Organic-only: text and image posting to a single connected account.
Explicitly does NOT build Threads Ads, and fetch_metrics() stays
honestly NotImplementedError -- analytics are out of scope for this
task, same pattern as every other connector.

Threads' OAuth flow needs no PKCE (unlike X's -- confirmed directly
against Meta's current Threads API docs, a plain authorization-code
exchange with a client secret), but state validation still genuinely
needs server-side round-trip state: the long-lived token itself is
obtained during the callback (a two-step exchange -- short-lived, then
immediately long-lived, see integrations/threads/api_client.py), and
must not sit in the browser-held Flask session while the account
picker/confirmation step (if any) completes. So, like XConnector,
authorize_url() and exchange_code() here take session/location_id/state
beyond the bare FlyerLadyConnector Protocol signature -- the same
justified deviation Instagram's own discover_accounts() override
already established as an accepted pattern for this Protocol.

No billing guard: unlike X, nothing in this task's requirements
indicates Threads posting is separately billed to VANTA, so none is
built here.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

from services.integration_status import integration_status
from integrations.threads.auth.config import ThreadsAuthConfig
from integrations.threads.auth.token_store import ThreadsTokenStore
from integrations.threads.api_client import ThreadsApiClient
from flyer_lady.publish_service import FlyerLadyPublishService

#: How long a pending Threads OAuth session (holding the freshly
#: exchanged long-lived token, before the connection row itself is
#: created) is held server-side. Same reasoning and duration as
#: XConnector's OAUTH_SESSION_TTL.
OAUTH_SESSION_TTL = timedelta(minutes=15)


class ThreadsConnector:
    platform_key = "threads"

    def __init__(self, config: ThreadsAuthConfig | None = None):
        self._config = config

    def _cfg(self) -> ThreadsAuthConfig:
        return self._config or ThreadsAuthConfig.from_env()

    # ------------------------------------------------------------------
    # OAuth, state validation
    # ------------------------------------------------------------------

    def authorize_url(self, *, redirect_uri: str, state: str, session=None, location_id: int | None = None) -> str:
        """Builds Threads' authorize URL. When session/location_id are
        given, also persists a ThreadsOAuthSession row keyed by
        `state` -- the CSRF state-validation record exchange_code()
        below looks up on the callback."""
        config = self._cfg()

        if session is not None and location_id is not None:
            from models.integration_models import ThreadsOAuthSession
            now = datetime.now(timezone.utc)
            session.add(ThreadsOAuthSession(
                location_id=location_id, state_nonce=state,
                redirect_uri=redirect_uri, status="started", expires_at=now + OAUTH_SESSION_TTL,
            ))
            session.flush()

        params = urlencode({
            "client_id": config.app_id, "redirect_uri": redirect_uri,
            "scope": ",".join(config.REQUIRED_SCOPES), "response_type": "code", "state": state,
        })
        return f"{ThreadsApiClient.AUTHORIZE_URL}?{params}"

    def exchange_code(self, *, code: str, redirect_uri: str, session=None, state: str | None = None) -> dict[str, Any]:
        """Exchanges the authorization code for a long-lived Threads
        token -- a two-step process (short-lived, then immediately
        long-lived) both handled here. State validation happens first,
        the same way as XConnector.exchange_code(): the
        ThreadsOAuthSession row is looked up by state_nonce == state,
        and this raises rather than proceeding if none matches, if it
        was already used, or if it has expired -- no token exchange is
        even attempted in any of those cases. The matched session is
        marked consumed once the long-lived token is obtained, so a
        replayed callback URL fails on a second attempt."""
        client = ThreadsApiClient(self._cfg())

        if session is not None and state is not None:
            from models.integration_models import ThreadsOAuthSession
            oauth = session.query(ThreadsOAuthSession).filter_by(state_nonce=state).one_or_none()
            if oauth is None:
                raise ValueError("Threads OAuth state did not match any pending authorization")
            if oauth.status != "started":
                raise ValueError("Threads OAuth session has already been used")
            if oauth.expires_at.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
                raise ValueError("Threads OAuth session has expired")

            short_lived = client.exchange_code_for_short_lived_token(code=code, redirect_uri=redirect_uri)
            long_lived = client.exchange_for_long_lived_token(short_lived["access_token"])

            oauth.status = "consumed"
            oauth.consumed_at = datetime.now(timezone.utc)
            session.flush()
            return long_lived

        raise ValueError("Threads token exchange requires session and state to validate the pending authorization")

    # ------------------------------------------------------------------
    # Account discovery
    # ------------------------------------------------------------------

    def discover_accounts(self, *, token: str) -> list[dict[str, Any]]:
        """GET /me -- like X, Threads' OAuth grant authorizes exactly
        one account per authorization; no Page/location picker step."""
        account = ThreadsApiClient(self._cfg()).get_me(token)
        return [account] if account else []

    # ------------------------------------------------------------------
    # Token refresh, revoke, health, capabilities
    # ------------------------------------------------------------------

    def refresh(self, *, refresh_token: str) -> str:
        """Threads has no separate refresh_token -- the long-lived
        access token itself is what GET /refresh_access_token extends.
        Named `refresh_token` here only to match the FlyerLadyConnector
        Protocol's parameter name; the value passed in is actually the
        current long-lived access token."""
        payload = ThreadsApiClient(self._cfg()).refresh_long_lived_token(refresh_token)
        return payload["access_token"]

    def revoke(self, session, *, location_id: int, **kwargs) -> dict[str, Any]:
        """No documented Threads-specific revoke endpoint exists
        (checked directly against Meta's current Threads API docs) --
        the same gap GoogleBusinessConnector.revoke() already
        documents for Google Business Profile. Marks the local
        connection revoked, which is what actually stops this location
        from publishing further."""
        from models.integration_models import ThreadsConnection
        connection = session.query(ThreadsConnection).filter_by(location_id=location_id).one_or_none()
        if connection is None:
            return {"connection_found": False}
        connection.connection_status = "revoked"
        session.flush()
        return {"connection_found": True, "connection_status": connection.connection_status}

    def health(self, session, location_id: int) -> dict[str, Any]:
        from models.integration_models import ThreadsConnection
        deployment = integration_status("threads")
        connection = session.query(ThreadsConnection).filter_by(location_id=location_id).one_or_none()
        return {
            "platform": self.platform_key,
            "deployment_configured": deployment["configured"],
            "deployment_missing": deployment["missing"],
            "connection_status": connection.connection_status if connection else "not_connected",
        }

    def capabilities(self) -> dict[str, Any]:
        """Organic text and image posting only -- the exact platform
        string flyer_lady/publish_service.py's threads_post branch
        dispatches on. Explicitly NOT Threads Ads or analytics."""
        return {"platform": "threads", "post_types": ["threads_post"]}

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def publish_step(self, session, location_id: int, post) -> Any:
        """Unchanged call: the same FlyerLadyPublishService.publish_post()
        every other connector's publish_step() calls."""
        return FlyerLadyPublishService().publish_post(session, location_id, post)

    def fetch_metrics(self, session, location_id: int) -> Any:
        raise NotImplementedError("Analytics are explicitly out of scope for the Threads connector")
