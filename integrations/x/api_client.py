"""X (Twitter) API v2 client -- OAuth 2.0 + PKCE token exchange/refresh/
revoke, account discovery, text and image publishing.

Endpoint and payload shapes verified directly against X's current
developer documentation (docs.x.com/fundamentals/authentication/
oauth-2-0/user-access-token and docs.x.com/x-api/posts/manage-tweets/
integrate) rather than assumed from memory -- notably, the v1.1 media
upload endpoints (upload.twitter.com/1.1/media/upload.json) this might
otherwise resemble were fully deprecated on 2025-06-09. Image upload
here uses the current v2 endpoint, POST /2/media/upload, which for a
single already-sized image (see flyer_lady/platforms/x_publisher.py)
is one plain multipart POST -- no INIT/APPEND/FINALIZE chunking, which
X's own docs describe as needed only for video and large files.

Mirrors integrations/google/business/api_client.py's shape exactly:
same _json_or_raise pattern, same XAPIError(status_code, error) shape
retry_policy.py's classifier already knows how to read.
"""
from __future__ import annotations

from typing import Any

import requests
from requests.auth import HTTPBasicAuth

from .auth.config import XAuthConfig


class XAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, error: dict[str, Any] | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.error = error or {}


class XApiClient:
    AUTHORIZE_URL = "https://x.com/i/oauth2/authorize"
    TOKEN_URL = "https://api.x.com/2/oauth2/token"
    REVOKE_URL = "https://api.x.com/2/oauth2/revoke"
    API_BASE = "https://api.x.com/2"

    def __init__(self, config: XAuthConfig, session: requests.Session | None = None):
        self.config = config
        self.session = session or requests.Session()

    # ------------------------------------------------------------------
    # OAuth 2.0 + PKCE
    # ------------------------------------------------------------------

    def exchange_code_for_tokens(self, *, code: str, redirect_uri: str, code_verifier: str) -> dict:
        """POST /2/oauth2/token, grant_type=authorization_code. X's
        confidential-client flow requires HTTP Basic auth
        (client_id:client_secret) on this call -- separate from Meta's
        and Google's token endpoints, which take client_id/secret as
        plain form fields instead."""
        response = self.session.post(
            self.TOKEN_URL,
            data={
                "code": code, "grant_type": "authorization_code",
                "redirect_uri": redirect_uri, "code_verifier": code_verifier,
                "client_id": self.config.client_id,
            },
            auth=HTTPBasicAuth(self.config.client_id, self.config.client_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        return self._json_or_raise(response, "X token exchange failed")

    def refresh_access_token(self, refresh_token: str) -> dict:
        """POST /2/oauth2/token, grant_type=refresh_token. Returns the
        full payload (X issues a NEW refresh token alongside the new
        access token on every refresh -- the old one is invalidated),
        not just the access token string, unlike
        GoogleBusinessApiClient.refresh_access_token()."""
        response = self.session.post(
            self.TOKEN_URL,
            data={"refresh_token": refresh_token, "grant_type": "refresh_token", "client_id": self.config.client_id},
            auth=HTTPBasicAuth(self.config.client_id, self.config.client_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        payload = self._json_or_raise(response, "X token refresh failed")
        if not payload.get("access_token"):
            raise XAPIError("X token refresh response had no access_token", error=payload)
        return payload

    def revoke_token(self, token: str) -> dict:
        response = self.session.post(
            self.REVOKE_URL,
            data={"token": token, "client_id": self.config.client_id},
            auth=HTTPBasicAuth(self.config.client_id, self.config.client_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        return self._json_or_raise(response, "X token revoke failed")

    # ------------------------------------------------------------------
    # Account discovery
    # ------------------------------------------------------------------

    def get_me(self, access_token: str) -> dict:
        """GET /2/users/me -- the authenticated account's own id and
        username. X's OAuth 2.0 user-context flow authorizes exactly
        one account per grant (there is no Page-picker step the way
        Meta's flow has); this is that one account's identity."""
        response = self.session.get(
            f"{self.API_BASE}/users/me",
            headers=self._auth_header(access_token), timeout=15,
        )
        payload = self._json_or_raise(response, "fetching the X account failed")
        return payload.get("data", {})

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def create_tweet(self, access_token: str, *, text: str, media_id: str | None = None) -> dict:
        body: dict[str, Any] = {"text": text}
        if media_id:
            body["media"] = {"media_ids": [media_id]}
        response = self.session.post(
            f"{self.API_BASE}/tweets",
            headers={**self._auth_header(access_token), "Content-Type": "application/json"},
            json=body, timeout=20,
        )
        return self._json_or_raise(response, "creating the X post failed")

    def upload_media(self, access_token: str, *, image_bytes: bytes, content_type: str = "image/jpeg") -> str:
        """POST /2/media/upload -- a single multipart request. Correct
        and sufficient for Flyer Lady's use case specifically: the
        existing media pipeline (integrations/storage/image_processing.py)
        already normalizes every image to a JPEG capped at 1600px on
        its longest edge before it ever reaches here, comfortably under
        the size where X's own docs require the INIT/APPEND/FINALIZE
        chunked flow (that flow is for video and large files -- this
        client does not implement it, since nothing in Flyer Lady's
        media pipeline produces anything that would need it)."""
        response = self.session.post(
            f"{self.API_BASE}/media/upload",
            headers=self._auth_header(access_token),
            files={"media": ("image.jpg", image_bytes, content_type)},
            data={"media_category": "tweet_image", "media_type": content_type},
            timeout=30,
        )
        payload = self._json_or_raise(response, "uploading the X image failed")
        media_id = (payload.get("data") or {}).get("id")
        if not media_id:
            raise XAPIError("X media upload response had no media id", error=payload)
        return media_id

    @staticmethod
    def _auth_header(access_token: str) -> dict:
        return {"Authorization": f"Bearer {access_token}"}

    @staticmethod
    def _json_or_raise(response: requests.Response, message: str) -> dict:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if not response.ok:
            error_detail = payload.get("error_description") or payload.get("title") or response.text[:300]
            raise XAPIError(f"{message}: {error_detail}", status_code=response.status_code, error=payload)
        return payload
