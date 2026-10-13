"""Google Ads OAuth client -- authorization-code exchange and access
token refresh against Google's standard OAuth 2.0 token endpoint.

Only the OAuth half exists so far. Campaign calls still go through
advertising/connectors/google_ads.py, which stays PLANNED; those will
also need GOOGLE_ADS_DEVELOPER_TOKEN, which plain OAuth does not.
"""
from __future__ import annotations

from typing import Any

import requests

from ..auth.config import GoogleAuthConfig


class GoogleAdsAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, error: dict[str, Any] | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.error = error or {}


class GoogleAdsApiClient:
    AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
    TOKEN_URL = "https://oauth2.googleapis.com/token"
    # The single scope Google documents for the Google Ads API.
    SCOPE = "https://www.googleapis.com/auth/adwords"

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
        return self._json_or_raise(response, "token exchange failed")

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
            raise GoogleAdsAPIError("token refresh response had no access_token", error=payload)
        return access_token

    @staticmethod
    def _json_or_raise(response: requests.Response, message: str) -> dict:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if not response.ok:
            error = payload.get("error")
            detail = (error.get("message") if isinstance(error, dict) else payload.get("error_description") or error) or response.text[:300]
            raise GoogleAdsAPIError(f"{message}: {detail}", status_code=response.status_code, error=payload)
        return payload
