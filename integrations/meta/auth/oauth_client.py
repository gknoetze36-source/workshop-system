"""Phase 4 public Meta OAuth configuration.

This module intentionally exposes no App Secret or System User token.
The actual Embedded Signup popup, config_id, callback/session handling and
authorization-code exchange are Phase 5 responsibilities.
"""
from __future__ import annotations

from dataclasses import dataclass

from .capability_config import WhatsAppMetaConfig


@dataclass(frozen=True)
class MetaOAuthConfiguration:
    app_id: str
    graph_api_version: str
    app_domains: tuple[str, ...]
    embedded_signup_config_id: str


class MetaOAuthClient:
    """Builds the safe configuration that a future frontend may consume.

    S12: WhatsApp Embedded Signup uses the WhatsApp Meta App's credentials
    explicitly. It must never fall back to Flyer Lady credentials, which
    WhatsAppMetaConfig makes structurally impossible -- it reads only
    META_WHATSAPP_* (and, transitionally, the legacy shared variables that
    belong to this same WhatsApp App).
    """

    def __init__(self, config: WhatsAppMetaConfig | None = None):
        self.config = config or WhatsAppMetaConfig.for_embedded_signup()

    def public_configuration(self) -> MetaOAuthConfiguration:
        return MetaOAuthConfiguration(
            app_id=self.config.app_id,
            graph_api_version=self.config.graph_api_version,
            app_domains=self.config.app_domains,
            embedded_signup_config_id=self.config.embedded_signup_config_id,
        )
