"""TikTok connector -- organic PHOTO Direct Post only, private/beta.

Explicitly does NOT build TikTok Ads, a video editor, analytics,
comments, or DMs. fetch_metrics() stays honestly NotImplementedError,
same pattern as every other connector in this registry.

No PKCE, unlike XConnector: confirmed against TikTok's own Login Kit
docs that the web/server (confidential client) flow doesn't use it --
see integrations/tiktok/auth/config.py's docstring. authorize_url() and
exchange_code() still take session/location_id/state beyond the bare
FlyerLadyConnector Protocol signature, the same justified deviation as
every other OAuth-holding connector in this registry, since state still
needs genuine server-side round-trip persistence for CSRF validation.

The privacy-level gate is this connector's one genuinely new piece:
confirm_privacy_level() is the only way selected_privacy_level ever
gets written, and it always re-validates against a fresh creator_info
call first -- never trusting a submitted value on its own, the same
"validate against what the platform actually returned" pattern this
codebase already established for Google's options_json validation
(models/integration_models.py's GoogleBusinessOAuthSession).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

from services.integration_status import integration_status
from integrations.tiktok.auth.config import TikTokAuthConfig
from integrations.tiktok.auth.token_store import TikTokTokenStore
from integrations.tiktok.api_client import TikTokApiClient
from flyer_lady.publish_service import FlyerLadyPublishService

OAUTH_SESSION_TTL = timedelta(minutes=15)


class TikTokConnector:
    platform_key = "tiktok"

    def __init__(self, config: TikTokAuthConfig | None = None):
        self._config = config

    def _cfg(self) -> TikTokAuthConfig:
        return self._config or TikTokAuthConfig.from_env()

    # ------------------------------------------------------------------
    # OAuth, state validation
    # ------------------------------------------------------------------

    def authorize_url(self, *, redirect_uri: str, state: str, session=None, location_id: int | None = None) -> str:
        config = self._cfg()

        if session is not None and location_id is not None:
            from models.integration_models import TikTokOAuthSession
            now = datetime.now(timezone.utc)
            session.add(TikTokOAuthSession(
                location_id=location_id, state_nonce=state,
                redirect_uri=redirect_uri, status="started", expires_at=now + OAUTH_SESSION_TTL,
            ))
            session.flush()

        params = urlencode({
            "client_key": config.client_key, "response_type": "code",
            "scope": ",".join(config.REQUIRED_SCOPES), "redirect_uri": redirect_uri, "state": state,
        })
        return f"{TikTokApiClient.AUTHORIZE_URL}?{params}"

    def exchange_code(self, *, code: str, redirect_uri: str, session=None, state: str | None = None) -> dict[str, Any]:
        """State validation happens first, same as every other
        OAuth-holding connector in this registry: the TikTokOAuthSession
        row is looked up by state_nonce == state, and this raises
        rather than proceeding for an unknown, already-used, or expired
        session. Returns the full token payload, including open_id and
        the refresh_token the caller must persist."""
        client = TikTokApiClient(self._cfg())

        if session is not None and state is not None:
            from models.integration_models import TikTokOAuthSession
            oauth = session.query(TikTokOAuthSession).filter_by(state_nonce=state).one_or_none()
            if oauth is None:
                raise ValueError("TikTok OAuth state did not match any pending authorization")
            if oauth.status != "started":
                raise ValueError("TikTok OAuth session has already been used")
            if oauth.expires_at.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
                raise ValueError("TikTok OAuth session has expired")

            payload = client.exchange_code_for_token(code=code, redirect_uri=redirect_uri)

            oauth.status = "consumed"
            oauth.consumed_at = datetime.now(timezone.utc)
            session.flush()
            return payload

        raise ValueError("TikTok token exchange requires session and state to validate the pending authorization")

    # ------------------------------------------------------------------
    # Account discovery, creator_info
    # ------------------------------------------------------------------

    def discover_accounts(self, *, token: str) -> list[dict[str, Any]]:
        """GET /v2/user/info/ -- TikTok's OAuth grant, like X's and
        Threads', authorizes exactly one account per authorization."""
        account = TikTokApiClient(self._cfg()).get_user_info(token)
        return [account] if account else []

    def get_creator_info(self, *, token: str) -> dict[str, Any]:
        """POST /v2/post/publish/creator_info/query/ -- required flow
        step: this is what supplies the creator_nickname to show the
        user and the privacy_level_options to build the (no-default)
        picker from. Not part of the bare FlyerLadyConnector Protocol;
        a genuine TikTok-specific step with no equivalent on the other
        four connectors."""
        return TikTokApiClient(self._cfg()).query_creator_info(token)

    def confirm_privacy_level(self, session, location_id: int, *, privacy_level: str) -> dict[str, Any]:
        """The only path that ever writes selected_privacy_level. Always
        re-validates against a fresh creator_info call first -- a
        submitted value is never trusted on its own, matching the same
        principle this codebase already applies to Google's
        options_json validation. Raises if the connection doesn't
        exist, or if the given level isn't currently one of this
        creator's allowed options."""
        from models.integration_models import TikTokConnection
        connection = session.query(TikTokConnection).filter_by(location_id=location_id).one_or_none()
        if connection is None:
            raise ValueError("No TikTok connection exists for this location")

        store = TikTokTokenStore()
        access_token = store.get_access_token(connection)
        creator_info = self.get_creator_info(token=access_token)
        allowed = creator_info.get("privacy_level_options") or []
        if privacy_level not in allowed:
            raise ValueError(
                f"{privacy_level!r} is not one of this creator's currently allowed privacy levels {allowed!r}"
            )

        connection.creator_nickname = creator_info.get("creator_nickname")
        connection.allowed_privacy_levels = allowed
        connection.selected_privacy_level = privacy_level
        session.flush()
        return {"creator_nickname": connection.creator_nickname, "selected_privacy_level": privacy_level}

    # ------------------------------------------------------------------
    # Token refresh, revoke, health, capabilities
    # ------------------------------------------------------------------

    def refresh(self, *, refresh_token: str) -> str:
        payload = TikTokApiClient(self._cfg()).refresh_token(refresh_token)
        return payload["access_token"]

    def revoke(self, session, *, location_id: int, **kwargs) -> dict[str, Any]:
        """No TikTok-specific revoke endpoint is used here -- TikTok
        does expose one (POST /v2/oauth/revoke/), but wiring it up is
        not among this task's required steps, and "keep this
        implementation isolated and small" argues against adding it
        speculatively. Marks the local connection revoked, the same
        honest-gap pattern GoogleBusinessConnector.revoke() and
        ThreadsConnector.revoke() already established in this
        registry."""
        from models.integration_models import TikTokConnection
        connection = session.query(TikTokConnection).filter_by(location_id=location_id).one_or_none()
        if connection is None:
            return {"connection_found": False}
        connection.connection_status = "revoked"
        session.flush()
        return {"connection_found": True, "connection_status": connection.connection_status}

    def health(self, session, location_id: int) -> dict[str, Any]:
        from models.integration_models import TikTokConnection
        deployment = integration_status("tiktok")
        connection = session.query(TikTokConnection).filter_by(location_id=location_id).one_or_none()
        privacy_level_confirmed = bool(connection.selected_privacy_level) if connection else False
        return {
            "platform": self.platform_key,
            "deployment_configured": deployment["configured"],
            "deployment_missing": deployment["missing"],
            "connection_status": connection.connection_status if connection else "not_connected",
            "privacy_level_confirmed": privacy_level_confirmed,
            # Same signal, named the way every other connector's health()
            # names its own version of "connected, but not actually able
            # to publish yet" -- see InstagramConnector.health() for the
            # other real case of this. flyer_lady/connectors/ui_status.py
            # reads this generic key uniformly across all six connectors
            # rather than hardcoding per-platform knowledge.
            "capability_ready": privacy_level_confirmed,
        }

    def capabilities(self) -> dict[str, Any]:
        """Organic PHOTO Direct Post only -- the exact platform string
        flyer_lady/publish_service.py's tiktok_post branch dispatches
        on. Explicitly NOT TikTok Ads, video, analytics, comments, or
        DMs."""
        return {"platform": "tiktok", "post_types": ["tiktok_post"]}

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def publish_step(self, session, location_id: int, post) -> Any:
        """Unchanged call: the same FlyerLadyPublishService.publish_post()
        every other connector's publish_step() calls."""
        return FlyerLadyPublishService().publish_post(session, location_id, post)

    def fetch_metrics(self, session, location_id: int) -> Any:
        raise NotImplementedError("Analytics are explicitly out of scope for the TikTok connector")
