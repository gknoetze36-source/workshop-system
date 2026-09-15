"""DEPRECATED -- the pre-separation shared Meta App configuration.

VANTA now runs TWO independent Meta Apps, each with its own credentials,
configuration ID, permissions, callbacks and App Review submission:

    integrations/meta/auth/capability_config.WhatsAppMetaConfig
    integrations/meta/auth/capability_config.FlyerLadyMetaConfig

This module is retained ONLY so the pre-separation unit tests
(tests/unit/test_meta_auth_phase4.py and siblings) continue to exercise the
original validation behaviour they were written against. No production code
imports it -- tests/security/test_meta_capability_separation.py's
test_no_production_code_uses_the_shared_meta_auth_config() parses every
non-test module and fails if that ever regresses.

S31: "production code MUST NOT use META_APP_ID or META_APP_SECRET as
universal credentials... The only allowed references should be migration/
backwards compatibility code if explicitly documented, tests, and migration
tooling." This file is that explicitly documented exception.

Do NOT import MetaAuthConfig in new code. It reads the shared
META_APP_ID/META_APP_SECRET, which after migration belong to the WhatsApp
App alone -- using it for Flyer Lady would silently authenticate as the
wrong Meta App (S47, S58).

The canonical definitions now live in capability_config and are re-exported
here so the two modules cannot drift apart.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from urllib.parse import urlparse

from .capability_config import (  # noqa: F401  (re-exported for legacy tests)
    DEFAULT_GRAPH_API_VERSION,
    WHATSAPP_REQUIRED_PERMISSIONS as REQUIRED_META_PERMISSIONS,
)

_GRAPH_VERSION_RE = re.compile(r"^v\d+\.\d+$")


@dataclass(frozen=True)
class MetaAuthConfig:
    app_id: str
    app_secret: str
    graph_api_version: str
    system_user_token: str = ""
    app_domains: tuple[str, ...] = ()
    embedded_signup_config_id: str = ""
    social_config_id: str = ""

    @classmethod
    def from_env(cls) -> "MetaAuthConfig":
        """Load the shared Meta App configuration.

        This deliberately does not require WhatsApp System User credentials,
        Embedded Signup configuration or Flyer Lady configuration.
        """
        config = cls(
            app_id=os.getenv("META_APP_ID", "").strip(),
            app_secret=os.getenv("META_APP_SECRET", "").strip(),
            graph_api_version=(
                os.getenv("META_GRAPH_API_VERSION", "").strip()
                or DEFAULT_GRAPH_API_VERSION
            ),
            system_user_token=os.getenv("META_SYSTEM_USER_TOKEN", "").strip(),
            app_domains=tuple(
                d.strip().rstrip("/")
                for d in os.getenv("META_APP_DOMAINS", "").split(",")
                if d.strip()
            ),
            embedded_signup_config_id=(
                os.getenv("META_WHATSAPP_CONFIG_ID", "").strip()
                or os.getenv("META_EMBEDDED_SIGNUP_CONFIG_ID", "").strip()
            ),
            social_config_id=os.getenv("META_FLYER_LADY_CONFIG_ID", "").strip(),
        )
        config.validate_shared()
        return config

    @classmethod
    def for_system_user(cls) -> "MetaAuthConfig":
        config = cls.from_env()
        config.validate_system_user()
        return config

    @classmethod
    def for_embedded_signup(cls) -> "MetaAuthConfig":
        config = cls.from_env()
        config.validate_embedded_signup()
        return config

    @classmethod
    def for_social(cls) -> "MetaAuthConfig":
        config = cls.from_env()
        config.validate_social()
        return config

    def graph_base_url(self) -> str:
        return f"https://graph.facebook.com/{self.graph_api_version}"

    def validate_shared(self) -> None:
        if not self.app_id:
            raise RuntimeError("META_APP_ID is required for the shared Meta App")
        if not self.app_secret:
            raise RuntimeError("META_APP_SECRET is required for the shared Meta App")
        if not self.app_id.isdigit():
            raise ValueError("META_APP_ID must be a numeric Meta App ID")
        if len(self.app_secret) < 16:
            raise ValueError("META_APP_SECRET appears invalid")
        if not _GRAPH_VERSION_RE.fullmatch(self.graph_api_version):
            raise ValueError(
                "META_GRAPH_API_VERSION must use the form vXX.X, for example v26.0"
            )
        self._validate_domains(required=False)

    def validate_system_user(self) -> None:
        self.validate_shared()
        if not self.system_user_token:
            raise ValueError("META_SYSTEM_USER_TOKEN is required for System User operations")
        if self.system_user_token.lower() in {"change-me", "replace-me", "your-token"}:
            raise ValueError("META_SYSTEM_USER_TOKEN is still using a placeholder value")

    def validate_embedded_signup(self) -> None:
        self.validate_shared()
        if not self.embedded_signup_config_id:
            raise RuntimeError("META_EMBEDDED_SIGNUP_CONFIG_ID is required for WhatsApp Embedded Signup")
        if not self.embedded_signup_config_id.isdigit():
            raise ValueError("META_EMBEDDED_SIGNUP_CONFIG_ID must be the numeric Facebook Login for Business config_id")
        self._validate_domains(required=True)

    def validate_social(self) -> None:
        self.validate_shared()
        if not self.social_config_id:
            raise RuntimeError("META_FLYER_LADY_CONFIG_ID is required for Flyer Lady social connection")

    def _validate_domains(self, *, required: bool) -> None:
        if required and not self.app_domains:
            raise RuntimeError("META_APP_DOMAINS must contain at least one HTTPS application domain")
        for domain in self.app_domains:
            parsed = urlparse(domain)
            if parsed.scheme != "https" or not parsed.netloc:
                raise ValueError(f"Meta app domain must be a valid HTTPS URL: {domain}")
            if parsed.username or parsed.password or parsed.query or parsed.fragment:
                raise ValueError(
                    f"Meta app domain must not contain credentials, query, or fragment: {domain}"
                )

    # Backwards-compatible full validation for callers that explicitly need
    # the historical Phase 4 System User contract.
    def validate(self) -> None:
        self.validate_system_user()

    @property
    def required_permissions(self) -> tuple[str, ...]:
        return REQUIRED_META_PERMISSIONS