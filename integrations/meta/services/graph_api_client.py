"""Single authenticated chokepoint for PHANTA Meta Graph API calls."""
from __future__ import annotations

from typing import Any, Mapping

import requests

from ..auth.config import MetaAuthConfig


class MetaGraphAPIError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.error = error or {}


class GraphApiClient:
    """Authenticated client for PHANTA Meta Graph API calls.

    App-level operations use PHANTA's System User token. Customer-scoped
    operations can explicitly supply the encrypted/decrypted Embedded Signup
    business token at the service layer. Tokens are never returned by this
    client and are not written to logs.
    """

    def __init__(
        self,
        config: MetaAuthConfig,
        session: requests.Session | None = None,
    ):
        config.validate()
        self.config = config
        self.session = session or requests.Session()

    def exchange_embedded_signup_code(self, code: str, *, timeout: float = 15.0) -> dict[str, Any]:
        if not code or len(code) > 4096:
            raise ValueError("Embedded Signup authorization code is required")
        url = self.config.graph_base_url() + "/oauth/access_token"
        params = {"client_id": self.config.app_id, "client_secret": self.config.app_secret, "code": code}
        try:
            response = self.session.get(url, params=params, headers={"Accept": "application/json"}, timeout=timeout)
        except requests.RequestException as exc:
            raise MetaGraphAPIError(f"Meta Embedded Signup token exchange failed: {exc}") from exc
        payload = self._json_payload(response)
        if not response.ok:
            error = payload.get("error", {}) if isinstance(payload, dict) else {}
            raise MetaGraphAPIError(error.get("message", f"Meta token exchange returned HTTP {response.status_code}"), status_code=response.status_code, error=error)
        if not isinstance(payload, dict) or not payload.get("access_token"):
            raise MetaGraphAPIError("Meta token exchange returned no access token")
        return payload

    def debug_customer_token(self, customer_token: str, *, timeout: float = 15.0) -> dict[str, Any]:
        if not customer_token:
            raise ValueError("customer_token is required")
        url = self.config.graph_base_url() + "/debug_token"
        params = {
            "input_token": customer_token,
            "access_token": f"{self.config.app_id}|{self.config.app_secret}",
        }
        try:
            response = self.session.get(url, params=params, headers={"Accept": "application/json"}, timeout=timeout)
        except requests.RequestException as exc:
            raise MetaGraphAPIError(f"Meta debug_token request failed: {exc}") from exc
        payload = self._json_payload(response)
        if not response.ok:
            error = payload.get("error", {}) if isinstance(payload, dict) else {}
            raise MetaGraphAPIError(error.get("message", f"Meta debug_token returned HTTP {response.status_code}"), status_code=response.status_code, error=error)
        if not isinstance(payload, dict):
            raise MetaGraphAPIError("Meta debug_token returned an unexpected response format")
        return payload

    def get(self, path: str, *, params: Mapping[str, Any] | None = None, timeout: float = 15.0) -> dict[str, Any]:
        return self._request("GET", path, params=params, timeout=timeout, token=self.config.system_user_token)

    def post(self, path: str, *, data: Mapping[str, Any] | None = None, timeout: float = 15.0) -> dict[str, Any]:
        return self._request("POST", path, data=data, timeout=timeout, token=self.config.system_user_token)

    def get_with_token(self, access_token: str, path: str, *, params: Mapping[str, Any] | None = None, timeout: float = 15.0) -> dict[str, Any]:
        return self._request("GET", path, params=params, timeout=timeout, token=access_token)

    def post_with_token(self, access_token: str, path: str, *, data: Mapping[str, Any] | None = None,
                        json_data: Mapping[str, Any] | None = None, timeout: float = 15.0) -> dict[str, Any]:
        return self._request("POST", path, data=data, json_data=json_data, timeout=timeout, token=access_token)

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        data: Mapping[str, Any] | None = None,
        json_data: Mapping[str, Any] | None = None,
        timeout: float = 15.0,
        token: str,
    ) -> dict[str, Any]:
        if not token:
            raise ValueError("Meta access token is required")
        if not path.startswith("/"):
            path = "/" + path
        if path.startswith("//") or "://" in path:
            raise ValueError("Meta Graph API path must be relative to the configured Graph API base URL")

        url = self.config.graph_base_url() + path
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        if json_data is not None:
            headers["Content-Type"] = "application/json"
        try:
            response = self.session.request(
                method, url, params=params, data=data, json=json_data,
                headers=headers, timeout=timeout
            )
        except requests.RequestException as exc:
            raise MetaGraphAPIError(f"Meta Graph API request failed: {exc}") from exc

        payload = self._json_payload(response)
        if not response.ok:
            error = payload.get("error", {}) if isinstance(payload, dict) else {}
            raise MetaGraphAPIError(error.get("message", f"Meta Graph API returned HTTP {response.status_code}"), status_code=response.status_code, error=error)
        if not isinstance(payload, dict):
            raise MetaGraphAPIError("Meta Graph API returned an unexpected response format")
        return payload

    @staticmethod
    def _json_payload(response: requests.Response) -> Any:
        try:
            return response.json()
        except ValueError:
            return {"raw": response.text}
