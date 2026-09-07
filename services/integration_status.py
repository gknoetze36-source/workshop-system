"""Capability-scoped integration configuration status.

Missing credentials for one Meta capability must not mark unrelated Meta
capabilities unavailable. This module reports variable names only.
"""
from __future__ import annotations

import os

META_APP_REQUIRED = ("META_APP_ID", "META_APP_SECRET")
META_SYSTEM_USER_REQUIRED = ("META_SYSTEM_USER_TOKEN",)
META_WEBHOOK_REQUIRED = ("META_WEBHOOK_VERIFY_TOKEN",)
META_EMBEDDED_SIGNUP_REQUIRED = ("META_APP_DOMAINS",)
META_SOCIAL_REQUIRED = ("META_FLYER_LADY_CONFIG_ID",)
GOOGLE_REQUIRED = ("GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET")
PAYSTACK_REQUIRED = ("PAYSTACK_SECRET_KEY",)


def _missing(names) -> list[str]:
    return [name for name in names if not (os.getenv(name) or "").strip()]


def integration_status(name: str) -> dict:
    groups = {
        "meta": META_APP_REQUIRED,
        "meta_app": META_APP_REQUIRED,
        "meta_system_user": META_APP_REQUIRED + META_SYSTEM_USER_REQUIRED,
        "meta_webhook": META_WEBHOOK_REQUIRED,
        # Runtime WhatsApp messaging is connection/token based per client.
        # It must not require Embedded Signup or Flyer Lady deployment config.
        "whatsapp": META_APP_REQUIRED,
        "embedded_signup": META_APP_REQUIRED + META_EMBEDDED_SIGNUP_REQUIRED,
        "flyer_lady": META_APP_REQUIRED + META_SOCIAL_REQUIRED,
        "google_business": GOOGLE_REQUIRED,
        "paystack": PAYSTACK_REQUIRED,
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
        )
    }


def require_configured(name: str):
    status = integration_status(name)
    if status["configured"]:
        return None
    return status, 503
