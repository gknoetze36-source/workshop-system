"""Threads API client -- OAuth token exchange/long-lived exchange/
refresh, account discovery, text and image publishing.

Endpoint shapes verified directly against Meta's current Threads API
documentation (developers.facebook.com/documentation/threads/get-started/
get-access-tokens-and-permissions and .../long-lived-tokens, and
developers.facebook.com/docs/threads/posts/ and
.../documentation/threads/threads-profiles) rather than assumed from
memory. One detail worth being explicit about, since it's easy to get
wrong: the short-lived token exchange happens on graph.threads.com, but
the long-lived exchange, refresh, account discovery, and publishing
endpoints are all on graph.threads.net -- a genuinely different domain,
confirmed directly against Meta's own pages rather than guessed as a
typo or inconsistency to normalize away.

Publishing is a two-step container model, structurally the same shape
as InstagramPublisher's existing container-then-poll-then-publish
pattern (flyer_lady/platforms/instagram_publisher.py) -- not reused
code (a different API, a different client, a different OAuth app
entirely), but the same proven shape, since Threads' own container
status values (EXPIRED, ERROR, FINISHED, IN_PROGRESS, PUBLISHED) map
almost exactly onto Instagram's.
"""
from __future__ import annotations

from typing import Any

import requests


class ThreadsAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, error: dict[str, Any] | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.error = error or {}


class ThreadsApiClient:
    AUTHORIZE_URL = "https://threads.com/oauth/authorize"
    SHORT_LIVED_TOKEN_URL = "https://graph.threads.com/oauth/access_token"
    LONG_LIVED_TOKEN_URL = "https://graph.threads.net/access_token"
    REFRESH_URL = "https://graph.threads.net/refresh_access_token"
    API_BASE = "https://graph.threads.net/v1.0"

    def __init__(self, config, session: requests.Session | None = None):
        self.config = config
        self.session = session or requests.Session()

    # ------------------------------------------------------------------
    # OAuth
    # ------------------------------------------------------------------

    def exchange_code_for_short_lived_token(self, *, code: str, redirect_uri: str) -> dict:
        """POST graph.threads.com/oauth/access_token, grant_type=authorization_code.
        Returns {access_token, token_type, user_id} -- the short-lived
        (1 hour) token, valid only long enough to immediately exchange
        for a long-lived one below."""
        response = self.session.post(
            self.SHORT_LIVED_TOKEN_URL,
            data={
                "client_id": self.config.app_id, "client_secret": self.config.app_secret,
                "grant_type": "authorization_code", "redirect_uri": redirect_uri, "code": code,
            },
            timeout=15,
        )
        return self._json_or_raise(response, "Threads token exchange failed")

    def exchange_for_long_lived_token(self, short_lived_token: str) -> dict:
        """GET graph.threads.net/access_token, grant_type=th_exchange_token.
        Returns {access_token, token_type, expires_in} -- valid 60
        days. Must be called with the short-lived token immediately;
        an expired short-lived token cannot be exchanged."""
        response = self.session.get(
            self.LONG_LIVED_TOKEN_URL,
            params={
                "grant_type": "th_exchange_token",
                "client_secret": self.config.app_secret,
                "access_token": short_lived_token,
            },
            timeout=15,
        )
        payload = self._json_or_raise(response, "Threads long-lived token exchange failed")
        if not payload.get("access_token"):
            raise ThreadsAPIError("Threads long-lived token exchange returned no access_token", error=payload)
        return payload

    def refresh_long_lived_token(self, long_lived_token: str) -> dict:
        """GET graph.threads.net/refresh_access_token, grant_type=th_refresh_token.
        Must be at least 24 hours old and not yet expired -- refreshing
        extends validity another 60 days from the refresh date. Returns
        a NEW token string; the old one is not usable after this."""
        response = self.session.get(
            self.REFRESH_URL,
            params={"grant_type": "th_refresh_token", "access_token": long_lived_token},
            timeout=15,
        )
        payload = self._json_or_raise(response, "Threads token refresh failed")
        if not payload.get("access_token"):
            raise ThreadsAPIError("Threads token refresh returned no access_token", error=payload)
        return payload

    # ------------------------------------------------------------------
    # Account discovery
    # ------------------------------------------------------------------

    def get_me(self, access_token: str) -> dict:
        """GET graph.threads.net/v1.0/me -- the authenticated account's
        own profile. Threads' OAuth grant, like X's, authorizes exactly
        one account per authorization; there is no Page-picker step."""
        response = self.session.get(
            f"{self.API_BASE}/me",
            params={
                "fields": "id,username,name,threads_profile_picture_url,threads_biography",
                "access_token": access_token,
            },
            timeout=15,
        )
        return self._json_or_raise(response, "fetching the Threads account failed")

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def create_container(self, threads_user_id: str, access_token: str, *, media_type: str, text: str | None = None, image_url: str | None = None) -> str:
        """POST /{threads-user-id}/threads. media_type is TEXT or IMAGE
        (VIDEO and CAROUSEL are not used by this connector -- organic
        text/image posting only, per this task's scope). Returns the
        new container's id."""
        data: dict[str, Any] = {"media_type": media_type, "access_token": access_token}
        if text:
            data["text"] = text
        if image_url:
            data["image_url"] = image_url
        response = self.session.post(f"{self.API_BASE}/{threads_user_id}/threads", data=data, timeout=20)
        payload = self._json_or_raise(response, "creating the Threads media container failed")
        container_id = payload.get("id")
        if not container_id:
            raise ThreadsAPIError("Threads container creation returned no id", error=payload)
        return str(container_id)

    def get_container_status(self, container_id: str, access_token: str) -> str:
        """GET /{threads-container-id}?fields=status -- one of EXPIRED,
        ERROR, FINISHED, IN_PROGRESS, PUBLISHED, per Meta's own
        documented values (developers.facebook.com/documentation/
        threads/troubleshooting)."""
        response = self.session.get(
            f"{self.API_BASE}/{container_id}",
            params={"fields": "status", "access_token": access_token},
            timeout=15,
        )
        payload = self._json_or_raise(response, "checking the Threads container status failed")
        return payload.get("status", "")

    def publish_container(self, threads_user_id: str, access_token: str, *, creation_id: str) -> str:
        """POST /{threads-user-id}/threads_publish. Returns the
        published Threads media id."""
        response = self.session.post(
            f"{self.API_BASE}/{threads_user_id}/threads_publish",
            data={"creation_id": creation_id, "access_token": access_token},
            timeout=20,
        )
        payload = self._json_or_raise(response, "publishing the Threads post failed")
        media_id = payload.get("id")
        if not media_id:
            raise ThreadsAPIError("Threads publish returned no media id", error=payload)
        return str(media_id)

    @staticmethod
    def _json_or_raise(response: requests.Response, message: str) -> dict:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if not response.ok:
            error_detail = payload.get("error_message") or (payload.get("error") or {}).get("message") or response.text[:300]
            raise ThreadsAPIError(f"{message}: {error_detail}", status_code=response.status_code, error=payload)
        return payload
