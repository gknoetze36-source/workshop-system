"""Integration configuration state.

THE PROBLEM THIS SOLVES
-----------------------
Meta and Google credentials come from environment variables. When they are not
set -- a fresh deployment, a local run, a Railway service missing a variable --
the configuration loaders raise:

    RuntimeError: Missing Meta authentication configuration: app_id, app_secret,
                  system_user_token
    RuntimeError: META_WEBHOOK_VERIFY_TOKEN is required

Nothing caught those, so the integration endpoints returned unhandled 500s. To
anyone using the system that reads as "the connections are broken", when the
actual situation is "this deployment has not been given its credentials yet" --
a completely different problem with a completely different fix.

It also violates the onboarding requirement that integration state be explicit
(NOT STARTED / IN PROGRESS / CONNECTED / FAILED / CANCELLED / REQUIRES ACTION)
and that PHANTA never misrepresent whether an integration is usable. A 500 is
not one of those states.

WHAT THIS DOES
--------------
Reports whether an integration is configured, WITHOUT raising and WITHOUT ever
returning the credential values themselves. Routes use it to answer "not
configured yet" clearly instead of crashing.

Only the presence of a variable is reported -- never its content -- so this is
safe to expose to an authenticated admin and safe to log.
"""
from __future__ import annotations

import os

# Variable -> whether the integration is unusable without it.
META_REQUIRED = ("META_APP_ID", "META_APP_SECRET", "META_SYSTEM_USER_TOKEN")
META_WEBHOOK_REQUIRED = ("META_WEBHOOK_VERIFY_TOKEN",)
META_WHATSAPP_REQUIRED = ("META_WHATSAPP_CONFIG_ID",)
META_SOCIAL_REQUIRED = ("META_FLYER_LADY_CONFIG_ID",)
GOOGLE_REQUIRED = ("GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET")
PAYSTACK_REQUIRED = ("PAYSTACK_SECRET_KEY",)


def _missing(names) -> list:
    return [name for name in names if not (os.getenv(name) or "").strip()]


def integration_status(name: str) -> dict:
    """Return {configured, missing, message} for one integration.

    `missing` lists variable NAMES only. Never values.
    """
    groups = {
        "meta": META_REQUIRED,
        "meta_webhook": META_WEBHOOK_REQUIRED,
        "whatsapp": META_REQUIRED + META_WHATSAPP_REQUIRED,
        "flyer_lady": META_REQUIRED + META_SOCIAL_REQUIRED,
        "google_business": GOOGLE_REQUIRED,
        "paystack": PAYSTACK_REQUIRED,
    }
    required = groups.get(name)
    if required is None:
        raise LookupError(f"unknown integration: {name}")

    missing = _missing(required)
    if not missing:
        return {"integration": name, "configured": True, "missing": [], "message": ""}

    return {
        "integration": name,
        "configured": False,
        "missing": missing,
        "message": (
            f"The {name.replace('_', ' ')} integration is not configured on this "
            f"deployment. Missing environment variable(s): {', '.join(missing)}. "
            "This is a PHANTA deployment setting, not a problem with your account."
        ),
    }


def all_integration_status() -> dict:
    """Configuration state of every integration, for diagnostics."""
    return {
        name: integration_status(name)
        for name in ("meta", "meta_webhook", "whatsapp", "flyer_lady",
                     "google_business", "paystack")
    }


def require_configured(name: str):
    """Return None when configured, or a (payload, status) tuple when not.

    Routes use this to answer 503 Service Unavailable with an explanation --
    the honest code for "this works, but it has not been set up here" -- rather
    than 500, which claims PHANTA itself failed.
    """
    status = integration_status(name)
    if status["configured"]:
        return None
    return status, 503
