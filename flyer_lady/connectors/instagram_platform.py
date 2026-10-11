"""Instagram connector -- Instagram API with Instagram Login.

Its own Meta app and its own connection (InstagramConnection), separate
from the Facebook Page connector in meta_social.py: a workshop connects
Instagram directly with its Instagram professional account, no Facebook
Page needed. Mirrors ThreadsConnector's shape.
"""
from __future__ import annotations

from typing import Any

from services.integration_status import integration_status
from integrations.instagram.config import InstagramAuthConfig
from integrations.instagram.api_client import InstagramLoginClient
from flyer_lady.publish_service import FlyerLadyPublishService


class InstagramConnector:
    platform_key = "instagram"

    def __init__(self, config: InstagramAuthConfig | None = None):
        self._config = config

    def _client(self) -> InstagramLoginClient:
        return InstagramLoginClient(self._config or InstagramAuthConfig.from_env())

    def capabilities(self) -> dict[str, Any]:
        return {"platform": "instagram", "post_types": ["instagram_feed", "instagram_story"]}

    def authorize_url(self, *, redirect_uri: str, state: str) -> str:
        return self._client().authorize_url(redirect_uri=redirect_uri, state=state)

    def exchange_code(self, *, code: str, redirect_uri: str) -> dict[str, Any]:
        client = self._client()
        short = client.exchange_code(code=code, redirect_uri=redirect_uri)
        return client.exchange_for_long_lived_token(short["access_token"])

    def discover_accounts(self, *, token: str) -> list[dict[str, Any]]:
        """Instagram Login authorizes exactly one account -- no picker."""
        return [self._client().get_me(token)]

    def refresh(self, *, refresh_token: str) -> str:
        return self._client().refresh_long_lived_token(refresh_token)["access_token"]

    def revoke(self, session, *, location_id: int, **kwargs) -> dict[str, Any]:
        """Instagram Login has no revoke endpoint; marking the connection
        revoked is what stops this location publishing (same as Threads)."""
        from models.integration_models import InstagramConnection
        connection = session.query(InstagramConnection).filter_by(location_id=location_id).one_or_none()
        if connection is None:
            return {"connection_found": False}
        connection.connection_status = "revoked"
        session.flush()
        return {"connection_found": True, "connection_status": connection.connection_status}

    def health(self, session, location_id: int) -> dict[str, Any]:
        from models.integration_models import InstagramConnection
        deployment = integration_status("instagram")
        connection = session.query(InstagramConnection).filter_by(location_id=location_id).one_or_none()
        return {
            "platform": self.platform_key,
            "deployment_configured": deployment["configured"],
            "deployment_missing": deployment["missing"],
            "connection_status": connection.connection_status if connection else "not_connected",
        }

    def publish_step(self, session, location_id: int, post) -> Any:
        return FlyerLadyPublishService().publish_post(session, location_id, post)

    def fetch_metrics(self, session, location_id: int) -> Any:
        raise NotImplementedError("No existing VANTA implementation fetches Flyer Lady metrics yet")
