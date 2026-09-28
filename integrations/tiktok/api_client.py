"""TikTok API client -- OAuth token exchange/refresh, account
discovery, creator_info, PHOTO Direct Post, and publish status
polling.

Endpoint shapes verified directly against TikTok's current developer
documentation (developers.tiktok.com/docs/en/login-kit-overview,
.../doc/oauth-user-access-token-management, .../docs/en/
display-api-get-started, .../doc/content-posting-api-get-started,
.../doc/content-posting-api-reference-photo-post, and .../doc/
content-posting-api-reference-get-video-status) rather than assumed
from memory. One detail worth flagging explicitly since third-party
sources disagree on it: TikTok's OAuth flow DOES use PKCE for
desktop/iOS/Android public clients, but NOT for the web/server flow
this client implements -- confirmed directly against TikTok's own
Login Kit overview page, which states the confidential-client model
(client_secret held server-side) makes PKCE unnecessary there. No PKCE
code is implemented in this client as a result.

This is a private/beta implementation: TikTok restricts all content
from an unaudited API client to SELF_ONLY viewership
(developers.tiktok.com/doc/content-sharing-guidelines), and its own UX
guidelines require the privacy level to be chosen from creator_info's
actual privacy_level_options with no default -- this client exposes
creator_info() and lets the caller (flyer_lady/connectors/
tiktok_platform.py) enforce that; it never picks a value itself.
"""
from __future__ import annotations

from typing import Any

import requests


class TikTokAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, error: dict[str, Any] | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.error = error or {}


class TikTokApiClient:
    AUTHORIZE_URL = "https://www.tiktok.com/v2/auth/authorize/"
    TOKEN_URL = "https://open.tiktokapis.com/v2/oauth/token/"
    API_BASE = "https://open.tiktokapis.com/v2"

    def __init__(self, config, session: requests.Session | None = None):
        self.config = config
        self.session = session or requests.Session()

    # ------------------------------------------------------------------
    # OAuth
    # ------------------------------------------------------------------

    def exchange_code_for_token(self, *, code: str, redirect_uri: str) -> dict:
        """POST /v2/oauth/token/, grant_type=authorization_code. Returns
        {access_token, expires_in, open_id, refresh_token,
        refresh_expires_in, scope, token_type}."""
        response = self.session.post(
            self.TOKEN_URL,
            data={
                "client_key": self.config.client_key, "client_secret": self.config.client_secret,
                "code": code, "grant_type": "authorization_code", "redirect_uri": redirect_uri,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded", "Cache-Control": "no-cache"},
            timeout=15,
        )
        return self._json_or_raise(response, "TikTok token exchange failed")

    def refresh_token(self, refresh_token: str) -> dict:
        """POST /v2/oauth/token/, grant_type=refresh_token. Same
        response shape as exchange_code_for_token(). TikTok's own docs
        are explicit: the returned refresh_token may differ from the
        one passed in, and the new one must be used going forward --
        this method returns the full payload precisely so the caller
        (integrations/tiktok/auth/token_store.py's
        save_connection_tokens(), always called with both tokens) can
        never accidentally keep the stale one."""
        response = self.session.post(
            self.TOKEN_URL,
            data={
                "client_key": self.config.client_key, "client_secret": self.config.client_secret,
                "grant_type": "refresh_token", "refresh_token": refresh_token,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded", "Cache-Control": "no-cache"},
            timeout=15,
        )
        payload = self._json_or_raise(response, "TikTok token refresh failed")
        if not payload.get("access_token") or not payload.get("refresh_token"):
            raise TikTokAPIError("TikTok token refresh response was missing a token", error=payload)
        return payload

    # ------------------------------------------------------------------
    # Account discovery
    # ------------------------------------------------------------------

    def get_user_info(self, access_token: str) -> dict:
        """GET /v2/user/info/ -- the authenticated account's own
        profile. TikTok's OAuth grant, like Threads' and X's,
        authorizes exactly one account per authorization."""
        response = self.session.get(
            f"{self.API_BASE}/user/info/",
            params={"fields": "open_id,union_id,avatar_url,display_name"},
            headers=self._auth_header(access_token),
            timeout=15,
        )
        payload = self._json_or_raise(response, "fetching the TikTok account failed")
        return payload.get("data", {}).get("user", payload.get("data", {}))

    # ------------------------------------------------------------------
    # creator_info -- required before every Direct Post
    # ------------------------------------------------------------------

    def query_creator_info(self, access_token: str) -> dict:
        """POST /v2/post/publish/creator_info/query/. Returns
        creator_nickname, creator_username, creator_avatar_url,
        privacy_level_options (the ONLY values this creator/app
        combination may legally publish with -- for an unaudited/
        private-beta client this is expected to be SELF_ONLY only),
        comment_disabled, duet_disabled, stitch_disabled,
        max_video_post_duration_sec. Must be called fresh before
        showing the privacy-level picker -- these options can change
        (e.g. if the creator's account visibility changes), which is
        exactly why "do not silently choose a privacy level" means
        showing THIS response's options, not a cached or assumed set.
        """
        response = self.session.post(
            f"{self.API_BASE}/post/publish/creator_info/query/",
            headers={**self._auth_header(access_token), "Content-Type": "application/json; charset=UTF-8"},
            timeout=15,
        )
        payload = self._json_or_raise(response, "fetching TikTok creator info failed")
        return payload.get("data", {})

    # ------------------------------------------------------------------
    # PHOTO Direct Post
    # ------------------------------------------------------------------

    def init_photo_post(
        self, access_token: str, *, title: str, privacy_level: str,
        photo_image_urls: list[str], description: str | None = None,
    ) -> str:
        """POST /v2/post/publish/content/init/, media_type=PHOTO,
        post_mode=DIRECT_POST, source=PULL_FROM_URL. privacy_level has
        no default and is a required keyword argument specifically so
        this method cannot be called without an explicit value -- the
        caller (flyer_lady/platforms/tiktok_publisher.py) is what
        validates it against creator_info()'s privacy_level_options
        before ever reaching here. Returns the new publish_id.

        PULL_FROM_URL requires the image URLs be reachable over public
        HTTPS -- exactly what Flyer Lady's existing media pipeline
        (integrations/storage/image_processing.py, R2-hosted) already
        produces for every image; no separate upload step is needed the
        way X's binary media upload required."""
        body: dict[str, Any] = {
            "media_type": "PHOTO",
            "post_mode": "DIRECT_POST",
            "post_info": {
                "title": title,
                "privacy_level": privacy_level,
                "disable_comment": True,
            },
            "source_info": {
                "source": "PULL_FROM_URL",
                "photo_cover_index": 0,
                "photo_images": photo_image_urls,
            },
        }
        if description:
            body["post_info"]["description"] = description
        response = self.session.post(
            f"{self.API_BASE}/post/publish/content/init/",
            headers={**self._auth_header(access_token), "Content-Type": "application/json; charset=UTF-8"},
            json=body, timeout=20,
        )
        payload = self._json_or_raise(response, "creating the TikTok photo post failed")
        publish_id = payload.get("data", {}).get("publish_id")
        if not publish_id:
            raise TikTokAPIError("TikTok photo post init returned no publish_id", error=payload)
        return publish_id

    # ------------------------------------------------------------------
    # Status polling
    # ------------------------------------------------------------------

    def fetch_publish_status(self, access_token: str, *, publish_id: str) -> dict:
        """POST /v2/post/publish/status/fetch/. Returns {status,
        fail_reason, publicaly_available_post_id, uploaded_bytes} --
        "publicaly" (missing the second "l") is TikTok's own field
        name, preserved exactly as their API returns it rather than
        corrected to what one might expect. status is one of
        PROCESSING_UPLOAD, PROCESSING_DOWNLOAD (PULL_FROM_URL's case),
        SEND_TO_USER_INBOX, PUBLISH_COMPLETE, FAILED."""
        response = self.session.post(
            f"{self.API_BASE}/post/publish/status/fetch/",
            headers={**self._auth_header(access_token), "Content-Type": "application/json; charset=UTF-8"},
            json={"publish_id": publish_id}, timeout=15,
        )
        payload = self._json_or_raise(response, "checking the TikTok post status failed")
        return payload.get("data", {})

    @staticmethod
    def _auth_header(access_token: str) -> dict:
        return {"Authorization": f"Bearer {access_token}"}

    @staticmethod
    def _json_or_raise(response: requests.Response, message: str) -> dict:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        error = payload.get("error") or {}
        error_code = error.get("code")
        if not response.ok or (error_code and error_code != "ok"):
            error_detail = error.get("message") or response.text[:300]
            raise TikTokAPIError(f"{message}: {error_detail}", status_code=response.status_code, error=payload)
        return payload
