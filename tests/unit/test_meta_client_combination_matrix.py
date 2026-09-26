"""Regression matrix for independent Meta capability configuration.

These tests model deployment-level configuration only. They intentionally do
not claim that a Meta account/token is live; database connection state and
Meta dashboard approval require integration/live tests.
"""
from services.integration_status import integration_status

META_KEYS = (
    "META_APP_ID", "META_APP_SECRET", "META_GRAPH_API_VERSION",
    "META_SYSTEM_USER_TOKEN", "META_APP_DOMAINS",
    "META_WHATSAPP_CONFIG_ID", "META_EMBEDDED_SIGNUP_CONFIG_ID",
    "META_FLYER_LADY_CONFIG_ID", "META_SOCIAL_REDIRECT_URI",
    "META_SOCIAL_OAUTH_SCOPES", "META_WEBHOOK_VERIFY_TOKEN",
    # tests/conftest.py sets these two globally via os.environ.setdefault
    # (needed by real call paths that construct a real GraphApiClient even
    # on a failure branch) -- not test-scoped, so a scenario asserting
    # "unconfigured" must explicitly clear them rather than assume blank.
    "META_WHATSAPP_APP_ID", "META_WHATSAPP_APP_SECRET",
    # services/integration_status.py's actual required vars for
    # flyer_lady's "configured" check.
    "META_FLYER_LADY_APP_ID", "META_FLYER_LADY_APP_SECRET",
)


def _env(monkeypatch, **values):
    for key in META_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def _shared(monkeypatch, **extra):
    values = {"META_APP_ID": "123456", "META_APP_SECRET": "x" * 32}
    values.update(extra)
    _env(monkeypatch, **values)


def test_scenario_1_whatsapp_configuration_does_not_require_social_or_embedded_signup(monkeypatch):
    # Runtime WhatsApp connection credentials are per-client/per-location.
    _shared(monkeypatch)
    assert integration_status("whatsapp")["configured"] is True
    assert integration_status("flyer_lady")["configured"] is False
    assert integration_status("embedded_signup")["configured"] is False


def test_scenario_2_facebook_flyer_lady_does_not_require_whatsapp_or_instagram(monkeypatch):
    # META_FLYER_LADY_APP_ID/SECRET are required alongside CONFIG_ID for
    # flyer_lady's own configured check since S43 (see
    # services/integration_status.py's own comment: "Previously this
    # required META_APP_ID/META_APP_SECRET" -- the shared/legacy vars were
    # deliberately removed from this check). Every "flyer_lady should be
    # configured" scenario below needs all three now; this scenario
    # previously omitted the App credentials and only appeared to pass
    # because some other test's leftover env state happened to supply them.
    _shared(monkeypatch, META_FLYER_LADY_CONFIG_ID="social-config", META_FLYER_LADY_APP_ID="789012", META_FLYER_LADY_APP_SECRET="y" * 32)
    assert integration_status("flyer_lady")["configured"] is True
    assert integration_status("embedded_signup")["configured"] is False
    assert integration_status("meta_system_user")["configured"] is False


def test_scenario_3_facebook_and_whatsapp_can_be_configured_without_instagram(monkeypatch):
    _shared(monkeypatch, META_FLYER_LADY_CONFIG_ID="social-config", META_FLYER_LADY_APP_ID="789012", META_FLYER_LADY_APP_SECRET="y" * 32)
    assert integration_status("whatsapp")["configured"] is True
    assert integration_status("flyer_lady")["configured"] is True
    # Instagram connection is account/database state, not a global deployment
    # requirement in the Facebook-connected model implemented by the app.


def test_scenario_4_current_code_does_not_artificially_require_whatsapp_for_social(monkeypatch):
    _shared(monkeypatch, META_FLYER_LADY_CONFIG_ID="social-config", META_FLYER_LADY_APP_ID="789012", META_FLYER_LADY_APP_SECRET="y" * 32)
    social = integration_status("flyer_lady")
    assert social["configured"] is True
    assert "META_SYSTEM_USER_TOKEN" not in social["missing"]
    assert not any("WHATSAPP_CONFIG" in item for item in social["missing"])


def test_scenario_5_facebook_and_instagram_model_configuration_does_not_require_whatsapp(monkeypatch):
    _shared(monkeypatch, META_FLYER_LADY_CONFIG_ID="social-config", META_FLYER_LADY_APP_ID="789012", META_FLYER_LADY_APP_SECRET="y" * 32)
    assert integration_status("flyer_lady")["configured"] is True
    assert integration_status("whatsapp")["configured"] is True
    # "whatsapp" here means the shared deployment layer is available; it does
    # not mean a client has connected a WhatsApp account.


def test_scenario_6_no_meta_integrations_reports_unconfigured_without_cross_failure(monkeypatch):
    _env(monkeypatch)
    for capability in ("meta_app", "whatsapp", "embedded_signup", "flyer_lady"):
        status = integration_status(capability)
        assert status["configured"] is False
        assert status["missing"]


def test_scenario_7_pending_embedded_signup_does_not_mark_flyer_lady_unconfigured(monkeypatch):
    _shared(monkeypatch, META_FLYER_LADY_CONFIG_ID="social-config", META_FLYER_LADY_APP_ID="789012", META_FLYER_LADY_APP_SECRET="y" * 32)
    assert integration_status("embedded_signup")["configured"] is False
    assert integration_status("flyer_lady")["configured"] is True
