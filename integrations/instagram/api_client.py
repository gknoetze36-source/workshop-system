"""Instagram API with Instagram Login: OAuth, account lookup, publishing.

Endpoints per Meta's Business Login for Instagram and Content Publishing
docs: authorize on www.instagram.com, code exchange on api.instagram.com,
everything else on graph.instagram.com. The three publishing methods keep
the names and return shapes InstagramPublisher already calls, so the
existing container -> poll -> publish logic is reused unchanged.
"""
from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

import requests


class InstagramAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, error: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.error = error or {}


class InstagramLoginClient:
    AUTHORIZE_URL = "https://www.instagram.com/oauth/authorize"
    CODE_EXCHANGE_URL = "https://api.instagram.com/oauth/access_token"
    GRAPH = "https://graph.instagram.com"

    def __init__(self, config, session: requests.Session | None = None):
        self.config = config
        self.session = session or requests.Session()

    @property
    def _api(self) -> str:
        return f"{self.GRAPH}/{self.config.graph_api_version}"

    # -- OAuth -------------------------------------------------------------

    def authorize_url(self, *, redirect_uri: str, state: str) -> str:
        return self.AUTHORIZE_URL + "?" + urlencode({
            "client_id": self.config.app_id, "redirect_uri": redirect_uri, "response_type": "code",
            "scope": ",".join(self.config.REQUIRED_SCOPES), "state": state,
        })

    def exchange_code(self, *, code: str, redirect_uri: str) -> dict:
        """Short-lived (1 hour) token. Meta returns either a flat object or
        {"data": [{access_token, user_id, permissions}]}; both are accepted."""
        payload = self._json(self.session.post(self.CODE_EXCHANGE_URL, data={
            "client_id": self.config.app_id, "client_secret": self.config.app_secret,
            "grant_type": "authorization_code", "redirect_uri": redirect_uri, "code": code,
        }, timeout=15), "Instagram code exchange failed")
        if isinstance(payload.get("data"), list) and payload["data"]:
            payload = payload["data"][0]
        if not payload.get("access_token"):
            raise InstagramAPIError("Instagram code exchange returned no access token", error=payload)
        return payload

    def exchange_for_long_lived_token(self, short_lived_token: str) -> dict:
        """60-day token: {access_token, token_type, expires_in}."""
        return self._token(self.session.get(f"{self.GRAPH}/access_token", params={
            "grant_type": "ig_exchange_token", "client_secret": self.config.app_secret,
            "access_token": short_lived_token,
        }, timeout=15), "Instagram long-lived token exchange failed")

    def refresh_long_lived_token(self, token: str) -> dict:
        """Extends a token that is at least 24 hours old and not expired by 60 days."""
        return self._token(self.session.get(f"{self.GRAPH}/refresh_access_token", params={
            "grant_type": "ig_refresh_token", "access_token": token,
        }, timeout=15), "Instagram token refresh failed")

    def get_me(self, token: str) -> dict:
        """user_id is the Instagram professional account ID used for publishing."""
        return self._json(self.session.get(f"{self._api}/me", params={
            "fields": "user_id,username", "access_token": token,
        }, timeout=15), "Fetching the Instagram account failed")

    # -- Publishing (same interface as MetaSocialGraphClient) --------------

    def create_instagram_container(self, ig_id: str, token: str, media_url: str, caption: str, *, stories: bool = False) -> dict:
        data: dict[str, Any] = {"image_url": media_url, "access_token": token}
        if stories:
            data["media_type"] = "STORIES"
        else:
            data["caption"] = caption
        return self._json(self.session.post(f"{self._api}/{ig_id}/media", data=data, timeout=20),
                          "Creating the Instagram media container failed")

    def get_instagram_container_status(self, token: str, creation_id: str) -> dict:
        return self._json(self.session.get(f"{self._api}/{creation_id}", params={
            "fields": "status_code", "access_token": token,
        }, timeout=15), "Checking the Instagram container status failed")

    def publish_instagram_container(self, ig_id: str, token: str, creation_id: str) -> dict:
        return self._json(self.session.post(f"{self._api}/{ig_id}/media_publish", data={
            "creation_id": creation_id, "access_token": token,
        }, timeout=20), "Publishing the Instagram post failed")

    # -- helpers -----------------------------------------------------------

    def _token(self, response, message: str) -> dict:
        payload = self._json(response, message)
        if not payload.get("access_token"):
            raise InstagramAPIError(message + ": no access token returned", error=payload)
        return payload

    @staticmethod
    def _json(response, message: str) -> dict:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if not response.ok:
            error = payload.get("error") if isinstance(payload.get("error"), dict) else {}
            detail = error.get("message") or payload.get("error_message") or str(response.status_code)
            # Meta reports an expired/revoked token as HTTP 400 code 190; surface
            # it as 401 so Flyer Lady's retry policy marks it reconnect-required.
            status = 401 if error.get("code") == 190 else response.status_code
            raise InstagramAPIError(f"{message}: {detail}", status_code=status, error=payload)
        return payload
