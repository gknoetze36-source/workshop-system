"""PHANTA Meta authentication foundation configuration.

Phase 4 owns the configuration contract for PHANTA's Meta app and its own
System User. Secrets are supplied later through Railway/environment secrets;
no real credentials belong in source control.

Embedded Signup customer authorization belongs to Phase 5 and customer-token
lifecycle belongs to Phase 6.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from urllib.parse import urlparse

DEFAULT_GRAPH_API_VERSION = "v26.0"
REQUIRED_META_PERMISSIONS = (
    "whatsapp_business_messaging",
    "whatsapp_business_management",
    "business_management",
)
_GRAPH_VERSION_RE = re.compile(r"^v\d+\.\d+$")


@dataclass(frozen=True)
class MetaAuthConfig:
    app_id: str
    app_secret: str
    graph_api_version: str
    system_user_token: str
    app_domains: tuple[str, ...]
    embedded_signup_config_id: str = ""
    social_config_id: str = ""

    @classmethod
    def from_env(cls) -> "MetaAuthConfig":
        values = {
            "app_id": os.getenv("META_APP_ID", "").strip(),
            "app_secret": os.getenv("META_APP_SECRET", "").strip(),
            "graph_api_version": (
                os.getenv("META_GRAPH_API_VERSION", "").strip()
                or DEFAULT_GRAPH_API_VERSION
            ),
            "system_user_token": os.getenv("META_SYSTEM_USER_TOKEN", "").strip(),
            "embedded_signup_config_id": (
                os.getenv("META_WHATSAPP_CONFIG_ID", "").strip()
                or os.getenv("META_EMBEDDED_SIGNUP_CONFIG_ID", "").strip()
            ),
            "social_config_id": os.getenv("META_FLYER_LADY_CONFIG_ID", "").strip(),
        }
        domains = tuple(
            d.strip().rstrip("/")
            for d in os.getenv("META_APP_DOMAINS", "").split(",")
            if d.strip()
        )

        missing = [key for key, value in values.items() if not value]
        if missing:
            raise RuntimeError(
                "Missing Meta authentication configuration: " + ", ".join(missing)
            )
        if not domains:
            raise RuntimeError(
                "META_APP_DOMAINS must contain at least one HTTPS application domain"
            )
        if not values["embedded_signup_config_id"]:
            raise RuntimeError("META_EMBEDDED_SIGNUP_CONFIG_ID is required for Phase 5")

        config = cls(**values, app_domains=domains)
        config.validate()
        return config

    def graph_base_url(self) -> str:
        return f"https://graph.facebook.com/{self.graph_api_version}"

    def validate(self) -> None:
        if not self.app_id.isdigit():
            raise ValueError("META_APP_ID must be a numeric Meta App ID")
        if len(self.app_secret) < 16:
            raise ValueError("META_APP_SECRET appears invalid")
        if not _GRAPH_VERSION_RE.fullmatch(self.graph_api_version):
            raise ValueError(
                "META_GRAPH_API_VERSION must use the form vXX.X, for example v26.0"
            )
        if not self.system_user_token:
            raise ValueError("META_SYSTEM_USER_TOKEN is required for Phase 4 health checks")
        if self.system_user_token.lower() in {"change-me", "replace-me", "your-token"}:
            raise ValueError("META_SYSTEM_USER_TOKEN is still using a placeholder value")
        if not self.app_domains:
            raise ValueError("At least one Meta application domain is required")

        for domain in self.app_domains:
            parsed = urlparse(domain)
            if parsed.scheme != "https" or not parsed.netloc:
                raise ValueError(f"Meta app domain must be a valid HTTPS URL: {domain}")
            if parsed.username or parsed.password or parsed.query or parsed.fragment:
                raise ValueError(
                    f"Meta app domain must not contain credentials, query, or fragment: {domain}"
                )
        if not self.embedded_signup_config_id or not self.embedded_signup_config_id.isdigit():
            raise ValueError("META_EMBEDDED_SIGNUP_CONFIG_ID must be the numeric Facebook Login for Business config_id")

    @property
    def required_permissions(self) -> tuple[str, ...]:
        return REQUIRED_META_PERMISSIONS
