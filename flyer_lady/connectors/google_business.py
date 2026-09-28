"""Google Business Profile connector.

Every method calls the exact function routes/google_business.py and
flyer_lady/publish_service.py already call:
  authorize_url      -> same URL shape as routes/google_business.py's connect_start()
  exchange_code      -> GoogleBusinessApiClient.exchange_code_for_tokens() (unchanged)
  discover_accounts  -> GoogleBusinessApiClient.list_accounts() + list_locations() (unchanged)
  refresh            -> GoogleBusinessApiClient.refresh_access_token() (unchanged)
  revoke             -> see below -- the one method with no existing per-platform
                         equivalent, written new, deliberately minimal
  health             -> integration_status("google_business") + the connection's own status
  publish_step       -> FlyerLadyPublishService.publish_post() (unchanged)
  fetch_metrics      -> NOT IMPLEMENTED, see below -- honest, not invented
"""
from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

from services.integration_status import integration_status
from integrations.google.auth.config import GoogleAuthConfig
from integrations.google.business.api_client import GoogleBusinessApiClient
from flyer_lady.publish_service import FlyerLadyPublishService


class GoogleBusinessConnector:
    platform_key = "google_business"

    def __init__(self, config: GoogleAuthConfig | None = None):
        self._config = config

    def _cfg(self) -> GoogleAuthConfig:
        return self._config or GoogleAuthConfig.from_env()

    def authorize_url(self, *, redirect_uri: str, state: str) -> str:
        """Builds the identical URL routes/google_business.py's
        connect_start() already builds. State is supplied by the
        caller, same reasoning as the Meta connectors -- CSRF/session
        management stays with the route."""
        config = self._cfg()
        params = urlencode({
            "client_id": config.client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "access_type": "offline",
            "prompt": "consent",
            "scope": GoogleBusinessApiClient.SCOPE,
            "state": state,
        })
        return f"https://accounts.google.com/o/oauth2/v2/auth?{params}"

    def exchange_code(self, *, code: str, redirect_uri: str) -> dict[str, Any]:
        """Unchanged call: GoogleBusinessApiClient.exchange_code_for_tokens(),
        the exact method the existing callback route already uses."""
        client = GoogleBusinessApiClient(self._cfg())
        return client.exchange_code_for_tokens(code, redirect_uri)

    def discover_accounts(self, *, token: str) -> list[dict[str, Any]]:
        """Unchanged calls: list_accounts() then list_locations() per
        account, the same two calls the existing callback route already
        makes to populate its picker."""
        client = GoogleBusinessApiClient(self._cfg())
        accounts = client.list_accounts(token)
        discovered = []
        for account in accounts:
            account_id = account.get("name")
            if not account_id:
                continue
            for location in client.list_locations(token, account_id):
                discovered.append({"account_id": account_id, **location})
        return discovered

    def refresh(self, *, refresh_token: str) -> str:
        """Unchanged call: GoogleBusinessApiClient.refresh_access_token(),
        the exact method flyer_lady/publish_service.py already calls
        before every Google Business publish attempt."""
        return GoogleBusinessApiClient(self._cfg()).refresh_access_token(refresh_token)

    def revoke(self, session, *, location_id: int, **kwargs) -> dict[str, Any]:
        """The one method with no existing per-platform equivalent to
        call into: services/offboarding_service.py has a
        _disconnect_integrations() that revokes Meta, Meta Social AND
        Google Business Profile together for a whole-location
        offboarding, which would be the wrong scope for a
        Google-specific revoke() here (it would also revoke Facebook/
        Instagram, which this connector has no business touching).
        Written new, deliberately minimal: the same single-column
        connection_status='revoked' update every one of the existing
        per-table revocations already performs, scoped correctly to
        just this location's Google Business connection."""
        from models.integration_models import GoogleBusinessConnection
        connection = session.query(GoogleBusinessConnection).filter_by(location_id=location_id).one_or_none()
        if connection is None:
            return {"connection_found": False}
        connection.connection_status = "revoked"
        session.flush()
        return {"connection_found": True, "connection_status": connection.connection_status}

    def health(self, session, location_id: int) -> dict[str, Any]:
        """Composes two existing signals: integration_status("google_business")
        (deployment-configured, unchanged) and this location's own
        GoogleBusinessConnection.connection_status, read directly."""
        from models.integration_models import GoogleBusinessConnection
        deployment = integration_status("google_business")
        connection = session.query(GoogleBusinessConnection).filter_by(location_id=location_id).one_or_none()
        return {
            "platform": self.platform_key,
            "deployment_configured": deployment["configured"],
            "deployment_missing": deployment["missing"],
            "connection_status": connection.connection_status if connection else "not_connected",
        }

    def capabilities(self) -> dict[str, Any]:
        """The exact platform string publish_service.py already
        dispatches on for Google -- one post type, Local Posts, which
        is all GoogleBusinessPublisher implements."""
        return {"platform": "google_business", "post_types": ["google_business_post"]}

    def publish_step(self, session, location_id: int, post) -> Any:
        """Unchanged call: FlyerLadyPublishService.publish_post() --
        the same entry point used for every platform, already
        dispatching correctly to the Google branch by post.platform."""
        return FlyerLadyPublishService().publish_post(session, location_id, post)

    def fetch_metrics(self, session, location_id: int) -> Any:
        """Not implemented -- no existing VANTA code fetches Google
        Business Profile Local Post metrics today."""
        raise NotImplementedError("No existing VANTA implementation fetches Google Business metrics yet")
