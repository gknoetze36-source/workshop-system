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


def test_flyer_lady_report_never_contains_the_secret(monkeypatch):
    import jobs.meta_preflight as mp
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "1082265940977910")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "abcdef0123456789abcdef0123456789")
    monkeypatch.setenv("META_FLYER_LADY_APP_DOMAINS", "https://app.vantaautomations.co.za")

    class Resp:
        status_code, content = 200, b"x"
        def json(self): return {"id": "1082265940977910", "name": "Vanta Automations FL"}
    seen = {}
    def fake_get(url, params, timeout):
        seen.update(params); return Resp()
    monkeypatch.setattr("requests.get", fake_get)
    report = mp._flyer_lady_report()
    assert report["ok"] and report["app_name"] == "Vanta Automations FL"
    assert "abcdef0123456789abcdef0123456789" not in repr(report)
