"""Capability-scoped integration configuration status.

Missing credentials for one Meta capability must not mark unrelated Meta
capabilities unavailable. This module reports variable names only.

VANTA runs TWO independent Meta Apps (S3). Neither capability may become
"unconfigured" merely because the other App is missing -- S43 makes that a
mandatory acceptance criterion. Each group below therefore names only its
own App's variables.
"""
from __future__ import annotations

import os

# --- Meta App A: WhatsApp -------------------------------------------------
# The legacy shared variables are accepted as a transitional fallback for
# WhatsApp only (S33/S34): the existing production Meta App keeps its
# identity, so those values belong to this App.
META_WHATSAPP_APP_REQUIRED = ("META_WHATSAPP_APP_ID", "META_WHATSAPP_APP_SECRET")
META_WHATSAPP_SYSTEM_USER_REQUIRED = ("META_WHATSAPP_SYSTEM_USER_TOKEN",)
META_WHATSAPP_WEBHOOK_REQUIRED = ("META_WHATSAPP_WEBHOOK_VERIFY_TOKEN",)
META_WHATSAPP_EMBEDDED_SIGNUP_REQUIRED = ("META_WHATSAPP_APP_DOMAINS",)

# --- Meta App B: Flyer Lady ----------------------------------------------
# Deliberately has NO legacy fallback. The Flyer Lady Meta App is new, so no
# legacy value could legitimately belong to it, and accepting one would
# report Flyer Lady as configured while it authenticated as the WhatsApp
# App (S47/S58).
META_FLYER_LADY_APP_REQUIRED = ("META_FLYER_LADY_APP_ID", "META_FLYER_LADY_APP_SECRET")
META_FLYER_LADY_SOCIAL_REQUIRED = ("META_FLYER_LADY_CONFIG_ID",)

GOOGLE_REQUIRED = ("GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET")
PAYSTACK_REQUIRED = ("PAYSTACK_SECRET_KEY",)
R2_REQUIRED = (
    "R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY",
    "R2_BUCKET_NAME", "R2_PUBLIC_BASE_URL",
)

# Transitional aliases, mirroring the application's own resolution order so
# this module agrees with what the running code will actually do.
_WHATSAPP_LEGACY_FALLBACKS = {
    "META_WHATSAPP_APP_ID": "META_APP_ID",
    "META_WHATSAPP_APP_SECRET": "META_APP_SECRET",
    "META_WHATSAPP_APP_DOMAINS": "META_APP_DOMAINS",
    "META_WHATSAPP_SYSTEM_USER_TOKEN": "META_SYSTEM_USER_TOKEN",
    "META_WHATSAPP_WEBHOOK_VERIFY_TOKEN": "META_WEBHOOK_VERIFY_TOKEN",
}


def _is_set(name: str) -> bool:
    if (os.getenv(name) or "").strip():
        return True
    legacy = _WHATSAPP_LEGACY_FALLBACKS.get(name)
    return bool(legacy and (os.getenv(legacy) or "").strip())


def _missing(names) -> list[str]:
    return [name for name in names if not _is_set(name)]


def integration_status(name: str) -> dict:
    groups = {
        # Kept for backwards compatibility with existing callers. Refers to
        # the WhatsApp App, which is what every current caller means by it.
        "meta": META_WHATSAPP_APP_REQUIRED,
        "meta_app": META_WHATSAPP_APP_REQUIRED,
        "meta_system_user": META_WHATSAPP_APP_REQUIRED + META_WHATSAPP_SYSTEM_USER_REQUIRED,
        "meta_webhook": META_WHATSAPP_WEBHOOK_REQUIRED,
        # Runtime WhatsApp messaging is connection/token based per client.
        # It must not require Embedded Signup or Flyer Lady deployment config.
        "whatsapp": META_WHATSAPP_APP_REQUIRED,
        "embedded_signup": META_WHATSAPP_APP_REQUIRED + META_WHATSAPP_EMBEDDED_SIGNUP_REQUIRED,
        # S43: names only Flyer Lady's own App. Previously this required
        # META_APP_ID/META_APP_SECRET, so Flyer Lady reported as
        # unconfigured whenever the WhatsApp App's credentials were absent.
        "flyer_lady": META_FLYER_LADY_APP_REQUIRED + META_FLYER_LADY_SOCIAL_REQUIRED,
        "google_business": GOOGLE_REQUIRED,
        "paystack": PAYSTACK_REQUIRED,
        "flyer_lady_uploads": R2_REQUIRED,
    }
    required = groups.get(name)
    if required is None:
        raise LookupError(f"unknown integration: {name}")

    missing = _missing(required)
    if name == "embedded_signup":
        config_id = (os.getenv("META_WHATSAPP_CONFIG_ID") or os.getenv("META_EMBEDDED_SIGNUP_CONFIG_ID") or "").strip()
        if not config_id:
            missing.append("META_WHATSAPP_CONFIG_ID or META_EMBEDDED_SIGNUP_CONFIG_ID")
    if not missing:
        return {"integration": name, "configured": True, "missing": [], "message": ""}

    return {
        "integration": name,
        "configured": False,
        "missing": missing,
        "message": (
            f"The {name.replace('_', ' ')} integration is not configured on this "
            f"deployment. Missing environment variable(s): {', '.join(missing)}. "
            "This is a deployment setting, not a problem with your account."
        ),
    }


def all_integration_status() -> dict:
    return {
        name: integration_status(name)
        for name in (
            "meta_app", "meta_system_user", "meta_webhook", "whatsapp",
            "embedded_signup", "flyer_lady", "google_business", "paystack",
            "flyer_lady_uploads",
        )
    }


def require_configured(name: str):
    status = integration_status(name)
    if status["configured"]:
        return None
    return status, 503
