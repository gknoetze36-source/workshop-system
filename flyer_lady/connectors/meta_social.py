"""Facebook and Instagram connectors.

Both wrap the SAME existing Meta Flyer Lady implementation -- one
Facebook Login grant connects a Page and (if linked) an Instagram
Business Account together; there is one MetaSocialConnection row per
location, one OAuth flow, one token. Facebook and Instagram differ only
in which of that connection's capabilities each one exposes, so
_MetaSocialConnectorBase holds everything genuinely shared, and the two
public classes below only override platform_key, capabilities(), and
(for Instagram) which of the discovered accounts are actually usable.

Every method calls the exact function routes/flyer_lady.py and
flyer_lady/publish_service.py already call:
  authorize_url   -> same URL shape as social_connect_start()
  exchange_code   -> the same GraphApiClient calls social_connect_callback() makes
  discover_accounts -> MetaSocialGraphClient.list_pages() (unchanged)
  refresh         -> NOT IMPLEMENTED, see below -- honest, not invented
  revoke          -> services.meta_data_deletion.deauthorize_flyer_lady_user() (unchanged)
  health          -> integration_status("flyer_lady") + the connection's own status
  publish_step    -> FlyerLadyPublishService.publish_post() (unchanged)
  fetch_metrics   -> NOT IMPLEMENTED, see below -- honest, not invented
"""
from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

from services.integration_status import integration_status
from integrations.meta.auth.capability_config import FlyerLadyMetaConfig
from integrations.meta.services.graph_api_client import GraphApiClient
from integrations.meta.social.graph_api_client import MetaSocialGraphClient
from integrations.meta.social.repositories.connection_repo import MetaSocialConnectionRepository
from services.meta_data_deletion import deauthorize_flyer_lady_user
from flyer_lady.publish_service import FlyerLadyPublishService


class _MetaSocialConnectorBase:
    platform_key = "meta_social"  # overridden by each subclass

    def __init__(self, config: FlyerLadyMetaConfig | None = None):
        self._config = config

    def _cfg(self) -> FlyerLadyMetaConfig:
        return self._config or FlyerLadyMetaConfig.from_env()

    def authorize_url(self, *, redirect_uri: str, state: str) -> str:
        """Builds the identical URL social_connect_start() (routes/flyer_lady.py)
        already builds -- same config, same scope source, same Graph
        API version. Does not touch flask_session at all: state is
        supplied by the caller, which is the route's job, not this
        connector's."""
        config = self._cfg()
        import os
        scopes = os.getenv("META_SOCIAL_OAUTH_SCOPES", ",".join(config.required_permissions))
        params = urlencode({
            "client_id": config.app_id,
            "config_id": config.facebook_login_config_id,
            "redirect_uri": redirect_uri,
            "state": state,
            "scope": scopes,
            "response_type": "code",
        })
        return f"https://www.facebook.com/{config.graph_api_version}/dialog/oauth?{params}"

    def exchange_code(self, *, code: str, redirect_uri: str) -> dict[str, Any]:
        """The exact same two-step exchange social_connect_callback()
        performs: the initial code exchange (a direct call on the same
        GraphApiClient session, since GraphApiClient has no separate
        named method for this specific endpoint), then
        exchange_for_long_lived_user_token() -- an existing, named
        client method, called exactly as the route already calls it."""
        config = self._cfg()
        client = GraphApiClient(config)
        response = client.session.get(
            config.graph_base_url() + "/oauth/access_token",
            params={"client_id": config.app_id, "client_secret": config.app_secret,
                    "redirect_uri": redirect_uri, "code": code},
            headers={"Accept": "application/json"}, timeout=15,
        )
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if not response.ok or not payload.get("access_token"):
            raise RuntimeError("Meta authorization token exchange failed")
        return client.exchange_for_long_lived_user_token(payload["access_token"])

    def discover_accounts(self, *, token: str) -> list[dict[str, Any]]:
        """Unchanged call: MetaSocialGraphClient.list_pages(), the exact
        method social_connect_callback() already uses to populate the
        page picker."""
        config = self._cfg()
        client = MetaSocialGraphClient(GraphApiClient(config))
        return client.list_pages(token).get("data", [])

    def refresh(self, *, refresh_token: str) -> str:
        """Not implemented -- and not a gap to close casually. Meta Page
        access tokens obtained through this flow are already long-lived
        (see the long-lived-user-token exchange in exchange_code()
        above); there is no existing VANTA code that refreshes a Page
        token on a schedule, because none has been needed yet. Raising
        here is honest about that, not a placeholder for logic that
        should exist."""
        raise NotImplementedError(
            "Meta Page tokens are long-lived; no existing refresh flow exists for them in VANTA"
        )

    def revoke(self, session, *, meta_user_id: str, **kwargs) -> dict[str, Any]:
        """Revokes with Meta itself first (DELETE /{user-id}/permissions,
        best-effort -- the same courtesy-call pattern
        XConnector.revoke() already uses), THEN the unchanged local
        call: services.meta_data_deletion.deauthorize_flyer_lady_user(),
        the exact function Meta's own data-deletion/deauthorization
        callbacks already use. Keyed by meta_user_id, not location_id,
        because that is what the existing function itself expects --
        this connector does not change that.

        Previously only did the local half: a workshop that
        "disconnected" Facebook/Instagram had its VANTA-side record
        marked revoked, but the token itself stayed live and usable on
        Meta's own side, since nothing ever told Meta the grant should
        end.
        """
        from sqlalchemy import select
        from models.integration_models import MetaSocialConnection
        from integrations.meta.auth.token_store import MetaTokenStore

        connections = session.scalars(
            select(MetaSocialConnection).where(MetaSocialConnection.meta_user_id == str(meta_user_id))
        ).all()
        if connections:
            store = MetaTokenStore()
            client = GraphApiClient(self._cfg())
            for connection in connections:
                try:
                    token = store.get_social_token(connection)
                    if token:
                        client.delete_with_token(token, f"/{meta_user_id}/permissions")
                except Exception:
                    # Best-effort courtesy call, same as
                    # XConnector.revoke(): if Meta's API is unreachable
                    # or the token is already invalid, the local revoke
                    # below still runs regardless, since that is the
                    # part that actually stops this location from
                    # publishing further.
                    pass

        return deauthorize_flyer_lady_user(session, meta_user_id)

    def health(self, session, location_id: int) -> dict[str, Any]:
        """Composes two existing signals, invents neither: whether
        Flyer Lady's Meta credentials are deployment-configured at all
        (integration_status("flyer_lady"), unchanged), and whether this
        specific location's own connection is currently connected
        (MetaSocialConnection.connection_status, read directly)."""
        deployment = integration_status("flyer_lady")
        connection = MetaSocialConnectionRepository().get_for_location(session, location_id)
        return {
            "platform": self.platform_key,
            "deployment_configured": deployment["configured"],
            "deployment_missing": deployment["missing"],
            "connection_status": connection.connection_status if connection else "not_connected",
        }

    def publish_step(self, session, location_id: int, post) -> Any:
        """Unchanged call: FlyerLadyPublishService.publish_post(), the
        exact method jobs/flyer_lady.py's scheduler and publish_now
        already call. It already dispatches correctly by post.platform
        on its own -- this connector adds no platform logic of its
        own."""
        return FlyerLadyPublishService().publish_post(session, location_id, post)

    def fetch_metrics(self, session, location_id: int) -> Any:
        """Not implemented for any platform -- no existing VANTA code
        fetches Flyer Lady post or Page metrics today. Raising here
        rather than returning an empty or fabricated result."""
        raise NotImplementedError("No existing VANTA implementation fetches Flyer Lady metrics yet")


class FacebookConnector(_MetaSocialConnectorBase):
    """Facebook Page feed and Story publishing -- the exact platform
    strings publish_service.py already dispatches on."""
    platform_key = "facebook"

    def capabilities(self) -> dict[str, Any]:
        return {"platform": "facebook", "post_types": ["facebook_feed", "facebook_story"]}


class InstagramConnector(_MetaSocialConnectorBase):
    """Instagram feed and Story publishing, via the SAME Facebook Page
    connection -- there is no separate Instagram OAuth in this
    codebase; Instagram is only usable when the connected Page has a
    linked Instagram Business Account."""
    platform_key = "instagram"

    def capabilities(self) -> dict[str, Any]:
        return {"platform": "instagram", "post_types": ["instagram_feed", "instagram_story"]}

    def discover_accounts(self, *, token: str) -> list[dict[str, Any]]:
        """Same underlying call as the base class -- list_pages() --
        filtered to Pages that actually have a linked Instagram
        Business Account, since an Instagram connector listing Pages
        with no Instagram account to publish to would be misleading."""
        pages = super().discover_accounts(token=token)
        return [p for p in pages if p.get("instagram_business_account")]

    def health(self, session, location_id: int) -> dict[str, Any]:
        """Adds one key beyond the base class: capability_ready. A
        MetaSocialConnection can genuinely be connection_status=
        "connected" (the Facebook Page OAuth succeeded) while still
        having no Instagram Business Account linked to that Page --
        publish_service.py's own instagram branch already checks
        connection.instagram_business_account_id and raises
        "Instagram Business Account is not connected" if it's missing.
        Surfacing that here is what lets the UI show Connect instead of
        a falsely-green Connected that would fail on the very next
        publish attempt."""
        result = super().health(session, location_id)
        connection = MetaSocialConnectionRepository().get_for_location(session, location_id)
        result["capability_ready"] = bool(connection and connection.instagram_business_account_id)
        return result
