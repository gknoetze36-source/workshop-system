"""Capability-scoped Meta App configuration (VANTA Meta App Separation, S30).

VANTA runs TWO independent Meta Apps:

  WhatsAppMetaConfig   -> "VANTA Automations - WhatsApp"
  FlyerLadyMetaConfig  -> "VANTA Automations - Flyer Lady"

These are separate Meta Apps with separate App IDs, App Secrets, Facebook
Login configurations, permissions, callbacks and App Review submissions.
They share only VANTA infrastructure (backend, database, domain, Business
Portfolio) and generic utility code -- never credentials.

Design notes, per the specification:

* S47 NO SILENT FALLBACK. Neither config ever reads the other capability's
  environment variables. If WhatsApp credentials are absent, WhatsApp fails
  with a WhatsApp-specific error while Flyer Lady continues to work, and
  vice versa. There is deliberately no cross-capability default.

* S5/S33 The legacy shared META_APP_ID / META_APP_SECRET are read ONLY by
  WhatsAppMetaConfig, and ONLY as an explicitly transitional fallback,
  because S34 requires the existing production WhatsApp Meta App to remain
  authoritative and untouched during migration. FlyerLadyMetaConfig never
  reads them: the Flyer Lady Meta App is new, so there is no legacy value
  that could legitimately belong to it, and allowing the fallback there
  would silently point Flyer Lady at the WhatsApp App -- exactly the
  coupling S58 forbids.

* S39 Both configs expose the same surface the shared MetaAuthConfig
  exposed to GraphApiClient (graph_base_url, validate_shared,
  validate_system_user) so the generic client can accept either without
  reaching into the environment to decide which credentials to use.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from urllib.parse import urlparse

DEFAULT_GRAPH_API_VERSION = "v26.0"

# S50: WhatsApp App Review permissions. business_management is required by
# the Tech Provider onboarding flow (WABA/System User assignment), not by
# messaging alone.
WHATSAPP_REQUIRED_PERMISSIONS = (
    "whatsapp_business_messaging",
    "whatsapp_business_management",
    "business_management",
)

# S18: Flyer Lady Facebook-only permissions. Deliberately minimal -- every
# permission here maps to a real code path in flyer_lady/:
#   pages_show_list       -> MetaSocialGraphClient.list_pages (/me/accounts)
#   pages_read_engagement -> reading Page context for the connected Page
#   pages_manage_posts    -> publish_feed_photo / publish_photo_story
# S18 explicitly forbids requesting pages_manage_metadata,
# pages_manage_engagement, pages_read_user_content, read_insights and
# pages_messaging without a real implementation, and S21 postpones the
# Instagram permissions entirely.
FLYER_LADY_REQUIRED_PERMISSIONS = (
    "pages_show_list",
    "pages_read_engagement",
    "pages_manage_posts",
)

_GRAPH_VERSION_RE = re.compile(r"^v\d+\.\d+$")
_PLACEHOLDER_VALUES = {"change-me", "replace-me", "your-token", "todo", "xxx"}


def _split_domains(raw: str) -> tuple[str, ...]:
    return tuple(d.strip().rstrip("/") for d in (raw or "").split(",") if d.strip())


def _validate_domains(domains: tuple[str, ...], *, required: bool, var_name: str) -> None:
    if required and not domains:
        raise RuntimeError(f"{var_name} must contain at least one HTTPS application domain")
    for domain in domains:
        parsed = urlparse(domain)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError(f"Meta app domain must be a valid HTTPS URL: {domain}")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError(
                f"Meta app domain must not contain credentials, query, or fragment: {domain}"
            )


def _validate_core(app_id: str, app_secret: str, graph_api_version: str, *, capability: str,
                   id_var: str, secret_var: str, version_var: str) -> None:
    """Shared credential shape checks, reported with capability-specific
    variable names so an operator is told exactly which App is misconfigured
    (S43/S54: neither capability may report the other's missing values)."""
    if not app_id:
        raise RuntimeError(f"{id_var} is required for the VANTA {capability} Meta App")
    if not app_secret:
        raise RuntimeError(f"{secret_var} is required for the VANTA {capability} Meta App")
    if not app_id.isdigit():
        raise ValueError(f"{id_var} must be a numeric Meta App ID")
    if len(app_secret) < 16:
        raise ValueError(f"{secret_var} appears invalid")
    if app_secret.lower() in _PLACEHOLDER_VALUES:
        raise ValueError(f"{secret_var} is still using a placeholder value")
    if not _GRAPH_VERSION_RE.fullmatch(graph_api_version):
        raise ValueError(f"{version_var} must use the form vXX.X, for example v26.0")


@dataclass(frozen=True)
class WhatsAppMetaConfig:
    """VANTA Automations - WhatsApp Meta App (S6)."""

    app_id: str
    app_secret: str
    graph_api_version: str
    system_user_token: str = ""
    app_domains: tuple[str, ...] = ()
    embedded_signup_config_id: str = ""
    webhook_verify_token: str = ""

    capability: str = field(default="WhatsApp", init=False, repr=False)

    @classmethod
    def from_env(cls) -> "WhatsAppMetaConfig":
        config = cls(
            # S33/S34: the legacy shared variables are a TRANSITIONAL
            # fallback for WhatsApp only, because the existing production
            # Meta App keeps its current App ID and Secret. Remove the
            # fallback once Railway is populated (deployment order S52
            # step 15).
            app_id=(
                os.getenv("META_WHATSAPP_APP_ID", "").strip()
                or os.getenv("META_APP_ID", "").strip()
            ),
            app_secret=(
                os.getenv("META_WHATSAPP_APP_SECRET", "").strip()
                or os.getenv("META_APP_SECRET", "").strip()
            ),
            graph_api_version=(
                os.getenv("META_WHATSAPP_GRAPH_API_VERSION", "").strip()
                or os.getenv("META_GRAPH_API_VERSION", "").strip()
                or DEFAULT_GRAPH_API_VERSION
            ),
            system_user_token=(
                os.getenv("META_WHATSAPP_SYSTEM_USER_TOKEN", "").strip()
                or os.getenv("META_SYSTEM_USER_TOKEN", "").strip()
            ),
            app_domains=_split_domains(
                os.getenv("META_WHATSAPP_APP_DOMAINS", "").strip()
                or os.getenv("META_APP_DOMAINS", "").strip()
            ),
            embedded_signup_config_id=(
                os.getenv("META_WHATSAPP_CONFIG_ID", "").strip()
                or os.getenv("META_EMBEDDED_SIGNUP_CONFIG_ID", "").strip()
            ),
            webhook_verify_token=(
                os.getenv("META_WHATSAPP_WEBHOOK_VERIFY_TOKEN", "").strip()
                or os.getenv("META_WEBHOOK_VERIFY_TOKEN", "").strip()
            ),
        )
        config.validate_shared()
        return config

    @classmethod
    def for_system_user(cls) -> "WhatsAppMetaConfig":
        config = cls.from_env()
        config.validate_system_user()
        return config

    @classmethod
    def for_embedded_signup(cls) -> "WhatsAppMetaConfig":
        config = cls.from_env()
        config.validate_embedded_signup()
        return config

    def graph_base_url(self) -> str:
        return f"https://graph.facebook.com/{self.graph_api_version}"

    def validate_shared(self) -> None:
        _validate_core(
            self.app_id, self.app_secret, self.graph_api_version,
            capability="WhatsApp",
            id_var="META_WHATSAPP_APP_ID",
            secret_var="META_WHATSAPP_APP_SECRET",
            version_var="META_WHATSAPP_GRAPH_API_VERSION",
        )
        _validate_domains(self.app_domains, required=False, var_name="META_WHATSAPP_APP_DOMAINS")

    def validate_system_user(self) -> None:
        self.validate_shared()
        if not self.system_user_token:
            raise ValueError(
                "META_WHATSAPP_SYSTEM_USER_TOKEN is required for WhatsApp System User operations"
            )
        if self.system_user_token.lower() in _PLACEHOLDER_VALUES:
            raise ValueError("META_WHATSAPP_SYSTEM_USER_TOKEN is still using a placeholder value")

    def validate_embedded_signup(self) -> None:
        self.validate_shared()
        if not self.embedded_signup_config_id:
            raise RuntimeError(
                "META_WHATSAPP_CONFIG_ID is required for WhatsApp Embedded Signup"
            )
        if not self.embedded_signup_config_id.isdigit():
            raise ValueError(
                "META_WHATSAPP_CONFIG_ID must be the numeric Facebook Login for Business config_id"
            )
        _validate_domains(self.app_domains, required=True, var_name="META_WHATSAPP_APP_DOMAINS")

    def validate_webhook(self) -> None:
        """S14/S15: webhook signature verification and the verify-token
        handshake both belong to the WhatsApp App."""
        self.validate_shared()
        if not self.webhook_verify_token:
            raise RuntimeError(
                "META_WHATSAPP_WEBHOOK_VERIFY_TOKEN is required for WhatsApp webhook verification"
            )

    @property
    def required_permissions(self) -> tuple[str, ...]:
        return WHATSAPP_REQUIRED_PERMISSIONS


@dataclass(frozen=True)
class FlyerLadyMetaConfig:
    """VANTA Automations - Flyer Lady Meta App (S7).

    Never reads META_APP_ID / META_APP_SECRET: see the module docstring.
    """

    app_id: str
    app_secret: str
    graph_api_version: str
    app_domains: tuple[str, ...] = ()
    facebook_login_config_id: str = ""
    oauth_redirect_uri: str = ""

    capability: str = field(default="Flyer Lady", init=False, repr=False)

    @classmethod
    def from_env(cls) -> "FlyerLadyMetaConfig":
        config = cls(
            app_id=os.getenv("META_FLYER_LADY_APP_ID", "").strip(),
            app_secret=os.getenv("META_FLYER_LADY_APP_SECRET", "").strip(),
            graph_api_version=(
                os.getenv("META_FLYER_LADY_GRAPH_API_VERSION", "").strip()
                or DEFAULT_GRAPH_API_VERSION
            ),
            app_domains=_split_domains(os.getenv("META_FLYER_LADY_APP_DOMAINS", "").strip()),
            facebook_login_config_id=os.getenv("META_FLYER_LADY_CONFIG_ID", "").strip(),
            oauth_redirect_uri=os.getenv("META_FLYER_LADY_OAUTH_REDIRECT_URI", "").strip(),
        )
        config.validate_shared()
        return config

    @classmethod
    def for_oauth(cls) -> "FlyerLadyMetaConfig":
        config = cls.from_env()
        config.validate_oauth()
        return config

    def graph_base_url(self) -> str:
        return f"https://graph.facebook.com/{self.graph_api_version}"

    def validate_shared(self) -> None:
        _validate_core(
            self.app_id, self.app_secret, self.graph_api_version,
            capability="Flyer Lady",
            id_var="META_FLYER_LADY_APP_ID",
            secret_var="META_FLYER_LADY_APP_SECRET",
            version_var="META_FLYER_LADY_GRAPH_API_VERSION",
        )
        _validate_domains(self.app_domains, required=False, var_name="META_FLYER_LADY_APP_DOMAINS")

    def validate_system_user(self) -> None:
        """S13/S58: Flyer Lady must never hold or use a WhatsApp System User
        token. GraphApiClient.get()/post() call this before using an
        app-level token, so raising here makes the forbidden path fail
        closed rather than silently reaching for WhatsApp credentials.
        Flyer Lady legitimately uses only get_with_token()/post_with_token()
        with a Page token, which does not call this.
        """
        raise RuntimeError(
            "Flyer Lady must not use WhatsApp System User credentials; "
            "use Page-token calls (get_with_token/post_with_token) instead"
        )

    def validate_oauth(self) -> None:
        self.validate_shared()
        if not self.facebook_login_config_id:
            raise RuntimeError(
                "META_FLYER_LADY_CONFIG_ID is required for the Flyer Lady Facebook Login configuration"
            )
        if not self.oauth_redirect_uri:
            raise RuntimeError(
                "META_FLYER_LADY_OAUTH_REDIRECT_URI is required for the Flyer Lady OAuth callback"
            )
        parsed = urlparse(self.oauth_redirect_uri)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("META_FLYER_LADY_OAUTH_REDIRECT_URI must be a valid HTTPS URL")
        # S36: the redirect URI must be used verbatim in both the
        # authorization request and the code exchange, so it must not carry
        # a query string or fragment that could drift between the two.
        if parsed.query or parsed.fragment:
            raise ValueError(
                "META_FLYER_LADY_OAUTH_REDIRECT_URI must not contain a query string or fragment"
            )

    @property
    def required_permissions(self) -> tuple[str, ...]:
        return FLYER_LADY_REQUIRED_PERMISSIONS
