"""Google Ads client: OAuth code exchange / token refresh, plus the three
Google Ads API (REST, v25) calls Flyer Lady uses -- listAccessibleCustomers,
googleAds:search (GAQL) and googleAds:mutate. Every Ads API call needs
GOOGLE_ADS_DEVELOPER_TOKEN on top of the OAuth access token.
"""
from __future__ import annotations

import os
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
    ADS_API = "https://googleads.googleapis.com/v25"

    def __init__(self, config: GoogleAuthConfig, session: requests.Session | None = None, developer_token: str | None = None):
        self.config = config
        self.session = session or requests.Session()
        self.developer_token = developer_token if developer_token is not None else os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "").strip()

    def _ads_headers(self, access_token: str) -> dict:
        if not self.developer_token:
            raise GoogleAdsAPIError("GOOGLE_ADS_DEVELOPER_TOKEN is not set")
        return {"Authorization": f"Bearer {access_token}", "developer-token": self.developer_token}

    def list_accessible_customers(self, access_token: str) -> list[str]:
        """Customer IDs (digits only) this login can reach directly."""
        response = self.session.get(f"{self.ADS_API}/customers:listAccessibleCustomers", headers=self._ads_headers(access_token), timeout=15)
        names = self._json_or_raise(response, "listing Google Ads accounts failed").get("resourceNames") or []
        return [n.split("/")[-1] for n in names]

    def search(self, access_token: str, customer_id: str, query: str) -> list[dict]:
        response = self.session.post(f"{self.ADS_API}/customers/{customer_id}/googleAds:search",
                                     headers=self._ads_headers(access_token), json={"query": query}, timeout=20)
        return self._json_or_raise(response, "Google Ads query failed").get("results") or []

    def mutate(self, access_token: str, customer_id: str, operations: list[dict]) -> list[dict]:
        """One atomic request: either every operation is applied or none is."""
        response = self.session.post(f"{self.ADS_API}/customers/{customer_id}/googleAds:mutate",
                                     headers=self._ads_headers(access_token), json={"mutateOperations": operations}, timeout=30)
        return self._json_or_raise(response, "Google Ads rejected the campaign").get("mutateOperationResponses") or []

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
            # Google Ads puts the specific, readable reason in error.details[].errors[].message
            for d in (error.get("details") or []) if isinstance(error, dict) else []:
                for e in d.get("errors") or []:
                    if e.get("message"):
                        detail = e["message"]; break
            raise GoogleAdsAPIError(f"{message}: {detail}", status_code=response.status_code, error=payload)
        return payload
