from integrations.meta.services.tech_provider_onboarding_service import (
    OnboardingStepError, StepOutcome, TechProviderOnboardingService,
)
from jobs.meta_preflight import run_meta_preflight


def _env(monkeypatch):
    monkeypatch.setenv("META_WHATSAPP_SYSTEM_USER_TOKEN", "EAAGsecret-token-value")
    monkeypatch.setenv("META_SYSTEM_USER_ID", "111")
    monkeypatch.setenv("META_BUSINESS_ID", "222")
    monkeypatch.setenv("META_CREDIT_SHARING_ENABLED", "false")


def test_reports_ok_without_leaking_the_token(monkeypatch):
    _env(monkeypatch)
    monkeypatch.setattr(TechProviderOnboardingService, "verify_provider_configuration",
                        lambda self: StepOutcome("provider_configuration", "performed", {"system_user_id": "111"}))
    report = run_meta_preflight()
    assert report["ok"] and report["system_user_id_resolved"] == "111"
    assert report["credit_sharing_enabled"] is False
    assert "EAAGsecret-token-value" not in repr(report)


def test_reports_the_failure_reason(monkeypatch):
    _env(monkeypatch)
    def boom(self):
        raise OnboardingStepError("waba_verified", "META_SYSTEM_USER_ID does not match")
    monkeypatch.setattr(TechProviderOnboardingService, "verify_provider_configuration", boom)
    report = run_meta_preflight()
    assert report["ok"] is False and "does not match" in report["error"]
