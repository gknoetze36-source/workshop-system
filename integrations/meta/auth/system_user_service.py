"""PHANTA's own Meta System User health checks.

This is Phase 4 foundation work. It does not implement customer token
exchange, encryption, expiry monitoring, or reconnection; those belong to
later Build Order phases.
"""
from __future__ import annotations

from typing import Any

from .config import MetaAuthConfig
from ..services.graph_api_client import GraphApiClient, MetaGraphAPIError


class SystemUserService:
    def __init__(
        self,
        config: MetaAuthConfig | None = None,
        client: GraphApiClient | None = None,
    ):
        self.config = config or MetaAuthConfig.from_env()
        self.client = client or GraphApiClient(self.config)

    def health_check(self) -> dict[str, Any]:
        """Verify the configured System User token can authenticate to Graph API.

        The response is deliberately safe for logging/dashboard use: the
        access token and App Secret are never returned.
        """
        try:
            result = self.client.get("/me", params={"fields": "id,name"})
            data = result.get("data", result) if isinstance(result, dict) else {}
            return {
                "healthy": bool(data.get("id")),
                "system_user_id": data.get("id"),
                "name": data.get("name"),
            }
        except MetaGraphAPIError as exc:
            return {
                "healthy": False,
                "system_user_id": None,
                "name": None,
                "error": {
                    "message": str(exc),
                    "status_code": exc.status_code,
                    "code": exc.error.get("code"),
                    "type": exc.error.get("type"),
                },
            }
