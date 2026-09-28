"""Check VANTA's own Meta Tech Provider credentials before any workshop needs them.

Reports only public identifiers (app, config, business and system user IDs)
and whether the System User token authenticates -- never a token or secret --
so Embedded Signup problems show up in the scheduler log, not mid-signup.
"""
from __future__ import annotations


def run_meta_preflight() -> dict:
    from integrations.meta.auth.capability_config import WhatsAppMetaConfig
    from integrations.meta.business.provider_config import MetaProviderConfig
    from integrations.meta.services.tech_provider_onboarding_service import (
        OnboardingStepError, TechProviderOnboardingService,
    )

    config = WhatsAppMetaConfig.from_env()
    provider = MetaProviderConfig.from_env()
    report = {
        "app_id": config.app_id,
        "embedded_signup_config_id": config.embedded_signup_config_id or None,
        "business_id": provider.business_id or None,
        "system_user_id_configured": provider.system_user_id or None,
        "system_user_token_present": bool(provider.system_user_token),
        "credit_sharing_enabled": provider.credit_sharing_enabled,
    }
    try:
        outcome = TechProviderOnboardingService(config=config, provider=provider).verify_provider_configuration()
        report.update(ok=True, system_user_id_resolved=outcome.detail.get("system_user_id"))
    except OnboardingStepError as exc:
        report.update(ok=False, error=str(exc))
    report["flyer_lady"] = _flyer_lady_report()
    return report


def _flyer_lady_report() -> dict:
    """Public Flyer Lady app settings, plus whether its App Secret is accepted.

    The secret is checked with an app access token call to /<app_id>, which
    only succeeds when META_FLYER_LADY_APP_SECRET belongs to that app. Neither
    the secret nor the token is included in the report.
    """
    import os
    import requests
    from integrations.meta.auth.capability_config import FlyerLadyMetaConfig

    raw = {
        "app_id": os.getenv("META_FLYER_LADY_APP_ID", "").strip() or None,
        "config_id": os.getenv("META_FLYER_LADY_CONFIG_ID", "").strip() or None,
        "oauth_redirect_uri": os.getenv("META_FLYER_LADY_OAUTH_REDIRECT_URI", "").strip() or None,
        "app_domains": os.getenv("META_FLYER_LADY_APP_DOMAINS", "").strip() or None,
    }
    try:
        config = FlyerLadyMetaConfig.from_env()
    except (RuntimeError, ValueError) as exc:
        return {**raw, "ok": False, "error": str(exc)}
    try:
        resp = requests.get(
            f"{config.graph_base_url()}/{config.app_id}",
            params={"fields": "id,name", "access_token": f"{config.app_id}|{config.app_secret}"},
            timeout=15,
        )
        body = resp.json() if resp.content else {}
    except Exception as exc:
        return {**raw, "ok": False, "error": f"secret check failed: {type(exc).__name__}"}
    if resp.status_code == 200:
        return {**raw, "ok": True, "app_name": body.get("name")}
    message = ((body.get("error") or {}).get("message") or f"HTTP {resp.status_code}")
    return {**raw, "ok": False, "error": message.replace(config.app_secret, "[redacted]")}
