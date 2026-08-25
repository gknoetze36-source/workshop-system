"""Google Business Profile API client -- OAuth token refresh and Local
Post creation.

Endpoint and payload shape verified directly against Google's own
current developer documentation (developers.google.com/my-business/
reference/rest/v4/accounts.locations.localPosts/create, last updated on
Google's site 2026-02-24) rather than assumed from memory -- the older
"Google My Business API" v4.9 endpoints this superficially resembles
were fully sunset on 2022-04-30 and stopped working entirely; this is
the still-live, still-current v4 Business Profile API (the product was
renamed, the mybusiness.googleapis.com domain and most of the v4 shape
were kept).
"""
from __future__ import annotations

from typing import Any

import requests

from ..auth.config import GoogleAuthConfig


class GoogleBusinessAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, error: dict[str, Any] | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.error = error or {}


class GoogleBusinessApiClient:
    TOKEN_URL = "https://oauth2.googleapis.com/token"
    API_BASE = "https://mybusiness.googleapis.com/v4"
    # business.manage is the scope Google's own docs list for localPosts
    # create/list; plus.business.manage is the older, deprecated alias.
    SCOPE = "https://www.googleapis.com/auth/business.manage"

    def __init__(self, config: GoogleAuthConfig, session: requests.Session | None = None):
        self.config = config
        self.session = session or requests.Session()

    def exchange_code_for_tokens(self, code: str, redirect_uri: str) -> dict:
        response = self.session.post(self.TOKEN_URL, data={
            "code": code,
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }, timeout=15)
        payload = self._json_or_raise(response, "token exchange failed")
        return payload

    def refresh_access_token(self, refresh_token: str) -> str:
        response = self.session.post(self.TOKEN_URL, data={
            "refresh_token": refresh_token,
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "grant_type": "refresh_token",
        }, timeout=15)
        payload = self._json_or_raise(response, "access token refresh failed")
        access_token = payload.get("access_token")
        if not access_token:
            raise GoogleBusinessAPIError("token refresh response had no access_token", error=payload)
        return access_token

    def list_accounts(self, access_token: str) -> list[dict]:
        response = self.session.get(
            "https://mybusinessaccountmanagement.googleapis.com/v1/accounts",
            headers=self._auth_header(access_token), timeout=15,
        )
        payload = self._json_or_raise(response, "listing Google Business accounts failed")
        return payload.get("accounts", [])

    def list_locations(self, access_token: str, account_id: str) -> list[dict]:
        response = self.session.get(
            f"https://mybusinessbusinessinformation.googleapis.com/v1/{account_id}/locations",
            headers=self._auth_header(access_token),
            params={"readMask": "name,title"},
            timeout=15,
        )
        payload = self._json_or_raise(response, "listing Google Business locations failed")
        return payload.get("locations", [])

    def create_local_post(self, access_token: str, account_id: str, location_id: str, *, summary: str, media_url: str | None = None, call_to_action_url: str | None = None) -> dict:
        body: dict[str, Any] = {"languageCode": "en-US", "summary": summary[:1500], "topicType": "STANDARD"}
        if media_url:
            body["media"] = [{"mediaFormat": "PHOTO", "sourceUrl": media_url}]
        if call_to_action_url:
            body["callToAction"] = {"actionType": "LEARN_MORE", "url": call_to_action_url}
        response = self.session.post(
            f"{self.API_BASE}/{account_id}/{location_id}/localPosts",
            headers=self._auth_header(access_token), json=body, timeout=20,
        )
        return self._json_or_raise(response, "creating the Google Business post failed")

    @staticmethod
    def _auth_header(access_token: str) -> dict:
        return {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    @staticmethod
    def _json_or_raise(response: requests.Response, message: str) -> dict:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if not response.ok:
            error_detail = (payload.get("error") or {}).get("message") or response.text[:300]
            raise GoogleBusinessAPIError(f"{message}: {error_detail}", status_code=response.status_code, error=payload)
        return payload
