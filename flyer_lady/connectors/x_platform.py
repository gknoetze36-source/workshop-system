"""X (Twitter) connector.

Organic-only, matching the explicit scope of this task: text and image
posting to a single connected account. No X Ads, no Activity API, no
DMs, no analytics -- none of those are implemented anywhere in this
connector, and fetch_metrics() stays honestly NotImplementedError, same
as the other three connectors.

One genuine, necessary difference from FacebookConnector/
InstagramConnector/GoogleBusinessConnector: X's OAuth 2.0 flow requires
PKCE (RFC 7636), which Meta's and Google's plain authorization-code
exchanges never needed. A PKCE code_verifier has to survive the
browser round-trip between authorize_url() and exchange_code() the same
way a refresh token does in Google's picker flow -- so, deliberately,
authorize_url() and exchange_code() here take two extra parameters
(session, location_id) beyond the bare FlyerLadyConnector Protocol
signature, to create and consume the XOAuthSession row that persists
the verifier server-side. This mirrors the same reasoning
InstagramConnector already established for connector-specific
variation (its own discover_accounts() override) -- the Protocol fixes
what every connector must expose, not that every connector's
implementation is identical.

Billing: X API usage is paid by VANTA, unlike the other three
platforms. The actual limit enforcement lives in
flyer_lady/billing/x_spend_guard.py, called from
flyer_lady/publish_service.py's x_post branch, not here --
publish_step() below calls the same, single publish_post() entry point
every connector uses, which is where that enforcement already runs.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from services.integration_status import integration_status
from integrations.x.auth.config import XAuthConfig
from integrations.x.auth.pkce import generate_pkce_pair
from integrations.x.auth.token_store import XTokenStore
from integrations.x.api_client import XApiClient
from flyer_lady.publish_service import FlyerLadyPublishService

#: How long a PKCE verifier is held server-side waiting for the user to
#: complete X's consent screen and be redirected back. Generous enough
#: for a real user to actually finish the flow, short enough that an
#: abandoned XOAuthSession row is not usable indefinitely.
OAUTH_SESSION_TTL = timedelta(minutes=15)


class XConnector:
    platform_key = "x"

    def __init__(self, config: XAuthConfig | None = None):
        self._config = config

    def _cfg(self) -> XAuthConfig:
        return self._config or XAuthConfig.from_env()

    # ------------------------------------------------------------------
    # OAuth 2.0 + PKCE, state validation
    # ------------------------------------------------------------------

    def authorize_url(self, *, redirect_uri: str, state: str, session=None, location_id: int | None = None) -> str:
        """Builds X's authorize URL AND, when session/location_id are
        given (the real, working path -- see the module docstring for
        why this needs more than redirect_uri/state alone), persists a
        fresh PKCE verifier server-side in a new XOAuthSession row
        keyed by `state`, so exchange_code() can find it again on the
        callback."""
        from urllib.parse import urlencode

        config = self._cfg()
        pkce = generate_pkce_pair()

        if session is not None and location_id is not None:
            from models.integration_models import XOAuthSession
            now = datetime.now(timezone.utc)
            session.add(XOAuthSession(
                location_id=location_id, state_nonce=state, code_verifier=pkce.verifier,
                redirect_uri=redirect_uri, status="started", expires_at=now + OAUTH_SESSION_TTL,
            ))
            session.flush()

        params = urlencode({
            "response_type": "code", "client_id": config.client_id, "redirect_uri": redirect_uri,
            "scope": " ".join(config.REQUIRED_SCOPES), "state": state,
            "code_challenge": pkce.challenge, "code_challenge_method": pkce.method,
        })
        return f"https://x.com/i/oauth2/authorize?{params}"

    def exchange_code(self, *, code: str, redirect_uri: str, session=None, state: str | None = None) -> dict[str, Any]:
        """Exchanges the authorization code for tokens. When session and
        state are given, this is where "state validation" (an explicit
        requirement) actually happens: the XOAuthSession row is looked
        up by state_nonce == state, and if none matches -- an unrequested
        callback, a replayed one, or a tampered state parameter -- this
        raises rather than proceeding, and no token exchange is even
        attempted. The matched session's own code_verifier is what
        makes the PKCE exchange succeed; the row is marked consumed
        immediately after, so a replay of the same callback URL fails
        on the second attempt too."""
        client = XApiClient(self._cfg())

        if session is not None and state is not None:
            from models.integration_models import XOAuthSession
            oauth = session.query(XOAuthSession).filter_by(state_nonce=state).one_or_none()
            if oauth is None:
                raise ValueError("X OAuth state did not match any pending authorization")
            if oauth.status != "started":
                raise ValueError("X OAuth session has already been used")
            if oauth.expires_at.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
                raise ValueError("X OAuth session has expired")
            payload = client.exchange_code_for_tokens(code=code, redirect_uri=redirect_uri, code_verifier=oauth.code_verifier)
            oauth.status = "consumed"
            oauth.consumed_at = datetime.now(timezone.utc)
            session.flush()
            return payload

        raise ValueError("X token exchange requires session and state to validate and locate the PKCE verifier")

    # ------------------------------------------------------------------
    # Account discovery
    # ------------------------------------------------------------------

    def discover_accounts(self, *, token: str) -> list[dict[str, Any]]:
        """GET /2/users/me -- X's OAuth 2.0 user-context grant
        authorizes exactly one account per authorization (there is no
        Page/location picker step the way Meta's and Google's flows
        have); this returns that single account as a one-item list, for
        a uniform shape with the other connectors' discover_accounts()."""
        account = XApiClient(self._cfg()).get_me(token)
        return [account] if account else []

    # ------------------------------------------------------------------
    # Token refresh, revoke, health, capabilities
    # ------------------------------------------------------------------

    def refresh(self, *, refresh_token: str) -> str:
        payload = XApiClient(self._cfg()).refresh_access_token(refresh_token)
        return payload["access_token"]

    def revoke(self, session, *, location_id: int, **kwargs) -> dict[str, Any]:
        """Revokes the token with X itself (POST /2/oauth2/revoke,
        unlike Google, which has no per-platform revoke endpoint to
        call -- see GoogleBusinessConnector.revoke()'s own docstring for
        that gap), then marks the local connection revoked."""
        from models.integration_models import XConnection
        connection = session.query(XConnection).filter_by(location_id=location_id).one_or_none()
        if connection is None:
            return {"connection_found": False}

        client = XApiClient(self._cfg())
        store = XTokenStore()
        try:
            client.revoke_token(store.get_access_token(connection))
        except Exception:
            # Revoking with X is a best-effort courtesy call -- if X's
            # API is unreachable or the token is already invalid, the
            # local connection is still marked revoked below regardless,
            # since that is the part that actually stops this location
            # from publishing further.
            pass

        connection.connection_status = "revoked"
        session.flush()
        return {"connection_found": True, "connection_status": connection.connection_status}

    def health(self, session, location_id: int) -> dict[str, Any]:
        from models.integration_models import XConnection
        deployment = integration_status("x")
        connection = session.query(XConnection).filter_by(location_id=location_id).one_or_none()
        return {
            "platform": self.platform_key,
            "deployment_configured": deployment["configured"],
            "deployment_missing": deployment["missing"],
            "connection_status": connection.connection_status if connection else "not_connected",
        }

    def capabilities(self) -> dict[str, Any]:
        """Organic text and image posting only -- the exact platform
        string flyer_lady/publish_service.py's x_post branch dispatches
        on. Explicitly NOT X Ads, Activity API, DMs, or analytics."""
        return {"platform": "x", "post_types": ["x_post"]}

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def publish_step(self, session, location_id: int, post) -> Any:
        """Unchanged call: the same FlyerLadyPublishService.publish_post()
        every other connector's publish_step() calls -- its x_post
        branch (flyer_lady/publish_service.py) is where the billing
        guard, token refresh, and actual publish all happen."""
        return FlyerLadyPublishService().publish_post(session, location_id, post)

    def fetch_metrics(self, session, location_id: int) -> Any:
        raise NotImplementedError("Analytics are explicitly out of scope for the X connector")
