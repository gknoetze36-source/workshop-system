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
    return report
