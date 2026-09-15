"""VANTA Meta App Separation -- configuration isolation tests.

Covers the S44 configuration matrix and the S62 failure-isolation
acceptance criteria for integrations/meta/auth/capability_config.py.

The point of these tests is S65: separation is NOT proven by the
environment variable names being different. It is proven by demonstrating
that each capability keeps working when the other's credentials are
absent, wrong, or deliberately swapped -- and that neither ever silently
reaches for the other's credentials.
"""
from __future__ import annotations

import pytest

from integrations.meta.auth.capability_config import (
    FlyerLadyMetaConfig,
    WhatsAppMetaConfig,
)

WHATSAPP_ID = "123456789"
WHATSAPP_SECRET = "w" * 32
FLYER_LADY_ID = "987654321"
FLYER_LADY_SECRET = "f" * 32
LEGACY_ID = "111111111"
LEGACY_SECRET = "l" * 32

_META_VARS = (
    "META_APP_ID", "META_APP_SECRET", "META_GRAPH_API_VERSION",
    "META_SYSTEM_USER_TOKEN", "META_APP_DOMAINS",
    "META_WHATSAPP_APP_ID", "META_WHATSAPP_APP_SECRET",
    "META_WHATSAPP_GRAPH_API_VERSION", "META_WHATSAPP_APP_DOMAINS",
    "META_WHATSAPP_CONFIG_ID", "META_WHATSAPP_SYSTEM_USER_TOKEN",
    "META_WHATSAPP_WEBHOOK_VERIFY_TOKEN", "META_EMBEDDED_SIGNUP_CONFIG_ID",
    "META_WEBHOOK_VERIFY_TOKEN",
    "META_FLYER_LADY_APP_ID", "META_FLYER_LADY_APP_SECRET",
    "META_FLYER_LADY_GRAPH_API_VERSION", "META_FLYER_LADY_APP_DOMAINS",
    "META_FLYER_LADY_CONFIG_ID", "META_FLYER_LADY_OAUTH_REDIRECT_URI",
)


@pytest.fixture
def clean_meta_env(monkeypatch):
    """Remove every Meta variable so each test starts from a known-empty
    environment rather than inheriting the developer's real credentials."""
    for var in _META_VARS:
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


def _configure_whatsapp(env):
    env.setenv("META_WHATSAPP_APP_ID", WHATSAPP_ID)
    env.setenv("META_WHATSAPP_APP_SECRET", WHATSAPP_SECRET)


def _configure_flyer_lady(env):
    env.setenv("META_FLYER_LADY_APP_ID", FLYER_LADY_ID)
    env.setenv("META_FLYER_LADY_APP_SECRET", FLYER_LADY_SECRET)


# --------------------------------------------------------------------------
# S44 TEST A: both configured -> both work
# --------------------------------------------------------------------------

def test_a_both_capabilities_configured(clean_meta_env):
    _configure_whatsapp(clean_meta_env)
    _configure_flyer_lady(clean_meta_env)

    assert WhatsAppMetaConfig.from_env().app_id == WHATSAPP_ID
    assert FlyerLadyMetaConfig.from_env().app_id == FLYER_LADY_ID


# --------------------------------------------------------------------------
# S44 TEST B / S62.1: Flyer Lady absent -> WhatsApp still works
# --------------------------------------------------------------------------

def test_b_whatsapp_works_without_any_flyer_lady_credentials(clean_meta_env):
    _configure_whatsapp(clean_meta_env)

    assert WhatsAppMetaConfig.from_env().app_id == WHATSAPP_ID

    with pytest.raises((RuntimeError, ValueError)) as exc:
        FlyerLadyMetaConfig.from_env()
    # S43/S54: the error must name the Flyer Lady variable, not a generic
    # or WhatsApp one, so an operator knows which App is misconfigured.
    assert "META_FLYER_LADY_APP_ID" in str(exc.value)


# --------------------------------------------------------------------------
# S44 TEST C / S62.2: WhatsApp absent -> Flyer Lady still works
# --------------------------------------------------------------------------

def test_c_flyer_lady_works_without_any_whatsapp_credentials(clean_meta_env):
    _configure_flyer_lady(clean_meta_env)

    assert FlyerLadyMetaConfig.from_env().app_id == FLYER_LADY_ID

    with pytest.raises((RuntimeError, ValueError)) as exc:
        WhatsAppMetaConfig.from_env()
    assert "META_WHATSAPP_APP_ID" in str(exc.value)


# --------------------------------------------------------------------------
# S44 TEST D/E: one capability's secret broken -> the other is unaffected
# --------------------------------------------------------------------------

def test_d_broken_whatsapp_secret_does_not_affect_flyer_lady(clean_meta_env):
    clean_meta_env.setenv("META_WHATSAPP_APP_ID", WHATSAPP_ID)
    clean_meta_env.setenv("META_WHATSAPP_APP_SECRET", "too-short")
    _configure_flyer_lady(clean_meta_env)

    with pytest.raises(ValueError):
        WhatsAppMetaConfig.from_env()
    assert FlyerLadyMetaConfig.from_env().app_id == FLYER_LADY_ID


def test_e_broken_flyer_lady_secret_does_not_affect_whatsapp(clean_meta_env):
    _configure_whatsapp(clean_meta_env)
    clean_meta_env.setenv("META_FLYER_LADY_APP_ID", FLYER_LADY_ID)
    clean_meta_env.setenv("META_FLYER_LADY_APP_SECRET", "too-short")

    with pytest.raises(ValueError):
        FlyerLadyMetaConfig.from_env()
    assert WhatsAppMetaConfig.from_env().app_id == WHATSAPP_ID


# --------------------------------------------------------------------------
# S44 TEST F/G: one capability's config ID broken -> the other is unaffected
# --------------------------------------------------------------------------

def test_f_broken_flyer_lady_config_id_does_not_affect_whatsapp(clean_meta_env):
    _configure_whatsapp(clean_meta_env)
    clean_meta_env.setenv("META_WHATSAPP_CONFIG_ID", "555")
    clean_meta_env.setenv("META_WHATSAPP_APP_DOMAINS", "https://app.vantaautomations.co.za")
    _configure_flyer_lady(clean_meta_env)
    # Flyer Lady config ID deliberately left unset.

    assert WhatsAppMetaConfig.for_embedded_signup().embedded_signup_config_id == "555"
    with pytest.raises(RuntimeError) as exc:
        FlyerLadyMetaConfig.for_oauth()
    assert "META_FLYER_LADY_CONFIG_ID" in str(exc.value)


def test_g_broken_whatsapp_config_id_does_not_affect_flyer_lady(clean_meta_env):
    _configure_whatsapp(clean_meta_env)
    # WhatsApp config ID deliberately left unset.
    _configure_flyer_lady(clean_meta_env)
    clean_meta_env.setenv("META_FLYER_LADY_CONFIG_ID", "777")
    clean_meta_env.setenv(
        "META_FLYER_LADY_OAUTH_REDIRECT_URI",
        "https://app.vantaautomations.co.za/dashboard/flyer-lady/connect/callback",
    )

    assert FlyerLadyMetaConfig.for_oauth().facebook_login_config_id == "777"
    with pytest.raises(RuntimeError) as exc:
        WhatsAppMetaConfig.for_embedded_signup()
    assert "META_WHATSAPP_CONFIG_ID" in str(exc.value)


# --------------------------------------------------------------------------
# S47 / S58: the no-silent-fallback rule -- the single most important test
# --------------------------------------------------------------------------

def test_flyer_lady_never_falls_back_to_legacy_shared_credentials(clean_meta_env):
    """S47 forbids any cross-capability fallback.

    The legacy META_APP_ID / META_APP_SECRET belong to the existing
    production WhatsApp Meta App (S34). If FlyerLadyMetaConfig read them,
    Flyer Lady would silently authenticate as the WhatsApp App -- the exact
    coupling S58 forbids, and a failure that would be invisible in
    production until Meta rejected the call.
    """
    clean_meta_env.setenv("META_APP_ID", LEGACY_ID)
    clean_meta_env.setenv("META_APP_SECRET", LEGACY_SECRET)

    with pytest.raises((RuntimeError, ValueError)) as exc:
        FlyerLadyMetaConfig.from_env()
    assert "META_FLYER_LADY_APP_ID" in str(exc.value)


def test_whatsapp_uses_legacy_shared_credentials_as_transitional_fallback(clean_meta_env):
    """S33/S34: WhatsApp keeps the existing production App ID and Secret, so
    reading the legacy variables is the intended migration path for this
    capability only. Removed at S52 step 15."""
    clean_meta_env.setenv("META_APP_ID", LEGACY_ID)
    clean_meta_env.setenv("META_APP_SECRET", LEGACY_SECRET)

    assert WhatsAppMetaConfig.from_env().app_id == LEGACY_ID


def test_explicit_whatsapp_variables_take_precedence_over_legacy(clean_meta_env):
    clean_meta_env.setenv("META_APP_ID", LEGACY_ID)
    clean_meta_env.setenv("META_APP_SECRET", LEGACY_SECRET)
    _configure_whatsapp(clean_meta_env)

    assert WhatsAppMetaConfig.from_env().app_id == WHATSAPP_ID


# --------------------------------------------------------------------------
# S13 / S58: Flyer Lady must never hold WhatsApp System User credentials
# --------------------------------------------------------------------------

def test_flyer_lady_refuses_system_user_operations(clean_meta_env):
    """GraphApiClient.get()/post() call validate_system_user() before using
    an app-level token. Flyer Lady legitimately uses only Page-token calls,
    so this path must fail closed rather than reach for WhatsApp's token."""
    _configure_flyer_lady(clean_meta_env)
    config = FlyerLadyMetaConfig.from_env()

    with pytest.raises(RuntimeError) as exc:
        config.validate_system_user()
    assert "must not use WhatsApp System User credentials" in str(exc.value)


def test_flyer_lady_config_has_no_system_user_token_attribute(clean_meta_env):
    """Structural guarantee: the field does not exist, so no code path can
    read a System User token off a Flyer Lady config even by mistake."""
    _configure_flyer_lady(clean_meta_env)
    assert not hasattr(FlyerLadyMetaConfig.from_env(), "system_user_token")


# --------------------------------------------------------------------------
# S18 / S21: Flyer Lady permission scope
# --------------------------------------------------------------------------

def test_flyer_lady_requests_only_demonstrable_facebook_permissions(clean_meta_env):
    _configure_flyer_lady(clean_meta_env)
    permissions = FlyerLadyMetaConfig.from_env().required_permissions

    assert set(permissions) == {
        "pages_show_list", "pages_read_engagement", "pages_manage_posts",
    }
    # S21: Instagram is postponed and must not appear in the initial
    # Facebook-only App Review submission.
    assert not any("instagram" in p for p in permissions)
    # S18: these are explicitly forbidden without a real implementation.
    for forbidden in ("pages_manage_metadata", "pages_manage_engagement",
                      "pages_read_user_content", "read_insights", "pages_messaging"):
        assert forbidden not in permissions


def test_whatsapp_does_not_request_facebook_page_permissions(clean_meta_env):
    """S50: Page permissions must not be added to the WhatsApp App merely
    because both products belong to VANTA."""
    _configure_whatsapp(clean_meta_env)
    permissions = WhatsAppMetaConfig.from_env().required_permissions

    assert not any(p.startswith("pages_") for p in permissions)
    assert not any("instagram" in p for p in permissions)


# --------------------------------------------------------------------------
# S36: the Flyer Lady redirect URI must be usable verbatim in both the
# authorization request and the code exchange
# --------------------------------------------------------------------------

@pytest.mark.parametrize("bad_uri", [
    "http://app.vantaautomations.co.za/dashboard/flyer-lady/connect/callback",  # not HTTPS
    "https://app.vantaautomations.co.za/callback?state=abc",                   # query string
    "https://app.vantaautomations.co.za/callback#frag",                        # fragment
    "not-a-url",
])
def test_flyer_lady_rejects_unusable_redirect_uris(clean_meta_env, bad_uri):
    _configure_flyer_lady(clean_meta_env)
    clean_meta_env.setenv("META_FLYER_LADY_CONFIG_ID", "777")
    clean_meta_env.setenv("META_FLYER_LADY_OAUTH_REDIRECT_URI", bad_uri)

    with pytest.raises(ValueError):
        FlyerLadyMetaConfig.for_oauth()


def test_flyer_lady_accepts_the_real_production_redirect_uri(clean_meta_env):
    _configure_flyer_lady(clean_meta_env)
    clean_meta_env.setenv("META_FLYER_LADY_CONFIG_ID", "777")
    clean_meta_env.setenv(
        "META_FLYER_LADY_OAUTH_REDIRECT_URI",
        "https://app.vantaautomations.co.za/dashboard/flyer-lady/connect/callback",
    )

    config = FlyerLadyMetaConfig.for_oauth()
    assert config.oauth_redirect_uri.endswith("/dashboard/flyer-lady/connect/callback")


# --------------------------------------------------------------------------
# S54: scripts/check_env.py must validate the two capabilities independently
# --------------------------------------------------------------------------

def _run_check_env(env_vars: dict) -> str:
    """Run check_env with ONLY the given variables set, capturing output."""
    import contextlib
    import io
    import os

    from scripts import check_env

    saved = dict(os.environ)
    try:
        for key in list(os.environ):
            del os.environ[key]
        os.environ.update(env_vars)
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            check_env.main([])
        return buffer.getvalue()
    finally:
        os.environ.clear()
        os.environ.update(saved)


WHATSAPP_ENV = {
    "META_WHATSAPP_APP_ID": WHATSAPP_ID,
    "META_WHATSAPP_APP_SECRET": WHATSAPP_SECRET,
    "META_WHATSAPP_APP_DOMAINS": "https://app.vantaautomations.co.za",
    "META_WHATSAPP_CONFIG_ID": "555",
    "META_WHATSAPP_SYSTEM_USER_TOKEN": "sysusertoken123456",
    "META_WHATSAPP_WEBHOOK_VERIFY_TOKEN": "verifytoken123",
}

FLYER_LADY_ENV = {
    "META_FLYER_LADY_APP_ID": FLYER_LADY_ID,
    "META_FLYER_LADY_APP_SECRET": FLYER_LADY_SECRET,
    "META_FLYER_LADY_CONFIG_ID": "777",
    "META_FLYER_LADY_OAUTH_REDIRECT_URI":
        "https://app.vantaautomations.co.za/dashboard/flyer-lady/connect/callback",
}


def test_check_env_reports_flyer_lady_ok_with_no_whatsapp_credentials():
    """S54: 'Flyer Lady missing WhatsApp System User token -> Flyer Lady
    check passes.' There must be no global 'Meta is configured' condition
    that requires both Apps."""
    output = _run_check_env(dict(FLYER_LADY_ENV))

    assert "OK    Meta App B — Flyer Lady" in output
    assert "OK    Flyer Lady Facebook Page connection" in output
    assert "--    Meta App A — WhatsApp" in output


def test_check_env_reports_whatsapp_ok_with_no_flyer_lady_credentials():
    output = _run_check_env(dict(WHATSAPP_ENV))

    assert "OK    Meta App A — WhatsApp" in output
    assert "OK    WhatsApp Embedded Signup (conditional)" in output
    assert "OK    WhatsApp inbound webhook (conditional)" in output
    assert "--    Meta App B — Flyer Lady" in output


def test_check_env_whatsapp_failures_never_name_flyer_lady_variables():
    """S43/S54: neither capability may report the other's missing values."""
    output = _run_check_env(dict(FLYER_LADY_ENV))

    whatsapp_lines = [
        line for line in output.splitlines()
        if "WhatsApp" in line and "missing:" in line
    ]
    assert whatsapp_lines, "expected WhatsApp integration lines to report as missing"
    for line in whatsapp_lines:
        assert "FLYER_LADY" not in line


def test_check_env_flyer_lady_failures_never_name_whatsapp_variables():
    output = _run_check_env(dict(WHATSAPP_ENV))

    flyer_lady_lines = [
        line for line in output.splitlines()
        if "Flyer Lady" in line and "missing:" in line and "R2" not in line
    ]
    assert flyer_lady_lines, "expected Flyer Lady integration lines to report as missing"
    for line in flyer_lady_lines:
        assert "META_WHATSAPP" not in line
        assert "META_APP_ID" not in line
        assert "META_APP_SECRET" not in line


def test_check_env_accepts_legacy_variables_for_whatsapp_during_migration():
    """S33: a production box still running on the old shared variables must
    not be reported as broken mid-migration."""
    output = _run_check_env({
        "META_APP_ID": LEGACY_ID,
        "META_APP_SECRET": LEGACY_SECRET,
        "META_APP_DOMAINS": "https://app.vantaautomations.co.za",
        "META_EMBEDDED_SIGNUP_CONFIG_ID": "555",
        "META_SYSTEM_USER_TOKEN": "legacysysuser123456",
        "META_WEBHOOK_VERIFY_TOKEN": "legacyverify123",
    })

    assert "OK    Meta App A — WhatsApp" in output
    assert "OK    WhatsApp Embedded Signup (conditional)" in output


def test_check_env_never_accepts_legacy_variables_for_flyer_lady():
    """S47: the legacy credentials belong to the WhatsApp app. Counting them
    as Flyer Lady configuration would report Flyer Lady as ready while it
    silently authenticated as the wrong Meta App."""
    output = _run_check_env({
        "META_APP_ID": LEGACY_ID,
        "META_APP_SECRET": LEGACY_SECRET,
        "META_SOCIAL_REDIRECT_URI":
            "https://app.vantaautomations.co.za/dashboard/flyer-lady/connect/callback",
    })

    assert "--    Meta App B — Flyer Lady" in output
    assert "--    Flyer Lady Facebook Page connection" in output


def test_check_env_warns_while_legacy_shared_variables_remain():
    """S31/S52 step 15: the final state must not depend on shared credentials."""
    output = _run_check_env({
        "META_APP_ID": LEGACY_ID,
        "META_APP_SECRET": LEGACY_SECRET,
    })

    assert "Legacy shared Meta variables are still set" in output


def test_check_env_never_prints_any_capability_secret_value():
    """Extends the existing secret-redaction guarantee to the new variables."""
    secret = "super-secret-value-do-not-print"
    output = _run_check_env({
        "FLASK_SECRET_KEY": secret,
        "META_APP_SECRET": secret,
        "META_WHATSAPP_APP_SECRET": secret,
        "META_FLYER_LADY_APP_SECRET": secret,
        "META_WHATSAPP_SYSTEM_USER_TOKEN": secret,
    })

    assert secret not in output


# --------------------------------------------------------------------------
# S14 / S15 / S62.5: WhatsApp webhook credential isolation
#
# routes/webhooks.py previously read META_APP_SECRET and
# META_WEBHOOK_VERIFY_TOKEN straight from the environment, making webhook
# verification depend on a generic Meta value. S14 requires the route "must
# not accept a generic Meta App Secret".
# --------------------------------------------------------------------------

def _reload_webhooks():
    import importlib

    import routes.webhooks as webhooks_module

    return importlib.reload(webhooks_module)


def test_webhook_signature_uses_the_whatsapp_app_secret(clean_meta_env):
    _configure_whatsapp(clean_meta_env)
    webhooks = _reload_webhooks()

    assert webhooks._whatsapp_app_secret() == WHATSAPP_SECRET


def test_webhook_rejects_a_flyer_lady_only_configuration(clean_meta_env):
    """S14/S62.5: the Flyer Lady App Secret must never satisfy the WhatsApp
    webhook check."""
    _configure_flyer_lady(clean_meta_env)
    webhooks = _reload_webhooks()

    with pytest.raises((RuntimeError, ValueError)):
        webhooks._whatsapp_app_secret()


def test_webhook_verify_token_comes_from_the_whatsapp_app(clean_meta_env):
    _configure_whatsapp(clean_meta_env)
    clean_meta_env.setenv("META_WHATSAPP_WEBHOOK_VERIFY_TOKEN", "verify-abc")
    webhooks = _reload_webhooks()

    assert webhooks._verify_token() == "verify-abc"


def test_webhook_still_works_on_legacy_variables_during_migration(clean_meta_env):
    """S33: a production deployment still running on the old shared
    variables must keep verifying webhooks correctly mid-migration."""
    clean_meta_env.setenv("META_APP_ID", LEGACY_ID)
    clean_meta_env.setenv("META_APP_SECRET", LEGACY_SECRET)
    clean_meta_env.setenv("META_WEBHOOK_VERIFY_TOKEN", "legacy-verify")
    webhooks = _reload_webhooks()

    assert webhooks._whatsapp_app_secret() == LEGACY_SECRET
    assert webhooks._verify_token() == "legacy-verify"


def test_flyer_lady_secret_fails_real_whatsapp_webhook_signature(clean_meta_env):
    """S62.5 end to end: sign a real body with the Flyer Lady secret and
    confirm the WhatsApp verifier rejects it, while a correctly signed body
    is accepted."""
    import hashlib
    import hmac
    import json

    from integrations.meta.webhook.signature_verifier import MetaSignatureVerifier

    _configure_whatsapp(clean_meta_env)
    webhooks = _reload_webhooks()

    body = json.dumps({"object": "whatsapp_business_account", "entry": []}).encode()

    def sign(secret: str) -> str:
        return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

    verifier = MetaSignatureVerifier(webhooks._whatsapp_app_secret())

    with pytest.raises(Exception):
        verifier.require_valid(body, sign(FLYER_LADY_SECRET))

    # Control: the correct WhatsApp signature must still be accepted.
    verifier.require_valid(body, sign(WHATSAPP_SECRET))


@pytest.mark.parametrize("bad_signature", [
    "sha256=" + "0" * 64,   # well-formed but wrong
    None,                   # missing
    "garbage",              # malformed
])
def test_webhook_rejects_bad_signatures(clean_meta_env, bad_signature):
    """S14 explicitly requires testing valid, invalid, missing and malformed
    signatures."""
    import json

    from integrations.meta.webhook.signature_verifier import MetaSignatureVerifier

    _configure_whatsapp(clean_meta_env)
    webhooks = _reload_webhooks()

    body = json.dumps({"object": "whatsapp_business_account", "entry": []}).encode()
    verifier = MetaSignatureVerifier(webhooks._whatsapp_app_secret())

    with pytest.raises(Exception):
        verifier.require_valid(body, bad_signature)


# --------------------------------------------------------------------------
# S31: no production WhatsApp code path may still use the shared config
# --------------------------------------------------------------------------

def test_whatsapp_services_are_wired_to_the_capability_config():
    """S12/S13: the WhatsApp services must construct WhatsAppMetaConfig, not
    the shared MetaAuthConfig."""
    import pathlib

    repo_root = pathlib.Path(__file__).resolve().parents[2]
    whatsapp_modules = [
        "integrations/meta/auth/oauth_client.py",
        "integrations/meta/auth/system_user_service.py",
        "integrations/meta/services/embedded_signup_service.py",
        "integrations/meta/services/token_status_service.py",
        "integrations/meta/services/tech_provider_onboarding_service.py",
        "integrations/meta/whatsapp/phone_number_service.py",
        "routes/webhooks.py",
    ]

    for rel_path in whatsapp_modules:
        source = (repo_root / rel_path).read_text()
        assert "MetaAuthConfig" not in source, (
            f"{rel_path} still references the shared MetaAuthConfig"
        )
        assert "WhatsAppMetaConfig" in source, (
            f"{rel_path} does not use WhatsAppMetaConfig"
        )


def test_graph_api_client_does_not_import_either_concrete_config():
    """S39: the shared client must never choose credentials itself. Typing it
    structurally means it cannot import, and therefore cannot construct,
    either capability config."""
    import pathlib

    repo_root = pathlib.Path(__file__).resolve().parents[2]
    source = (repo_root / "integrations/meta/services/graph_api_client.py").read_text()

    assert "from ..auth.config import MetaAuthConfig" not in source
    assert "WhatsAppMetaConfig" not in source
    assert "FlyerLadyMetaConfig" not in source
    assert "MetaCapabilityConfig" in source


# --------------------------------------------------------------------------
# S43: integration_status must validate each capability independently.
# This is a MANDATORY acceptance criterion.
# --------------------------------------------------------------------------

def test_integration_status_flyer_lady_ok_without_whatsapp(clean_meta_env):
    """Previously "flyer_lady" required META_APP_ID/META_APP_SECRET, so Flyer
    Lady reported as unconfigured whenever the WhatsApp App's credentials
    were absent."""
    from services.integration_status import integration_status

    _configure_flyer_lady(clean_meta_env)
    clean_meta_env.setenv("META_FLYER_LADY_CONFIG_ID", "777")

    assert integration_status("flyer_lady")["configured"] is True
    assert integration_status("whatsapp")["configured"] is False


def test_integration_status_whatsapp_ok_without_flyer_lady(clean_meta_env):
    from services.integration_status import integration_status

    _configure_whatsapp(clean_meta_env)

    assert integration_status("whatsapp")["configured"] is True
    assert integration_status("flyer_lady")["configured"] is False


def test_integration_status_failures_never_name_the_other_capability(clean_meta_env):
    from services.integration_status import integration_status

    _configure_whatsapp(clean_meta_env)
    missing = integration_status("flyer_lady")["missing"]

    assert "META_APP_ID" not in missing
    assert "META_APP_SECRET" not in missing
    assert "META_WHATSAPP_APP_ID" not in missing
    assert all(name.startswith("META_FLYER_LADY_") for name in missing)


def test_integration_status_legacy_vars_configure_whatsapp_only(clean_meta_env):
    """S33 + S47 together: legacy credentials satisfy WhatsApp during
    migration, and never satisfy Flyer Lady."""
    from services.integration_status import integration_status

    clean_meta_env.setenv("META_APP_ID", LEGACY_ID)
    clean_meta_env.setenv("META_APP_SECRET", LEGACY_SECRET)
    clean_meta_env.setenv("META_FLYER_LADY_CONFIG_ID", "777")

    assert integration_status("whatsapp")["configured"] is True
    assert integration_status("flyer_lady")["configured"] is False


# --------------------------------------------------------------------------
# S25 / S26 / S62.6: data deletion uses the Flyer Lady App Secret
# --------------------------------------------------------------------------

def _make_signed_request(secret: str, user_id: str = "123") -> str:
    """Build a Meta-shaped signed_request. Meta includes `algorithm`, and the
    parser correctly rejects a payload without it."""
    import base64
    import hashlib
    import hmac
    import json

    body = {"algorithm": "HMAC-SHA256", "user_id": user_id}
    payload = base64.urlsafe_b64encode(json.dumps(body).encode()).decode().rstrip("=")
    signature = hmac.new(secret.encode(), payload.encode("ascii"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(signature).decode().rstrip("=") + "." + payload


def test_whatsapp_secret_fails_a_flyer_lady_signed_request(clean_meta_env):
    """S62.6: "Use WhatsApp App Secret against Flyer Lady signed request ->
    Request is rejected." """
    from services.meta_data_deletion import InvalidSignedRequest, parse_signed_request

    _configure_flyer_lady(clean_meta_env)
    flyer_lady_secret = FlyerLadyMetaConfig.from_env().app_secret

    with pytest.raises(InvalidSignedRequest):
        parse_signed_request(_make_signed_request(WHATSAPP_SECRET), flyer_lady_secret)

    # Control: the correct Flyer Lady signature must still be accepted.
    payload = parse_signed_request(_make_signed_request(FLYER_LADY_SECRET), flyer_lady_secret)
    assert payload["user_id"] == "123"


def test_data_deletion_does_not_resolve_a_whatsapp_only_configuration(clean_meta_env):
    """S25: the Flyer Lady App Secret must never be substituted by the
    WhatsApp one."""
    _configure_whatsapp(clean_meta_env)

    with pytest.raises((RuntimeError, ValueError)):
        FlyerLadyMetaConfig.from_env()


# --------------------------------------------------------------------------
# S27 / S28 / S29: deauthorization boundary and audit naming
# --------------------------------------------------------------------------

def _code_only(source: str) -> str:
    """Strip comments and docstrings so a source assertion tests real code.

    Without this, an explanatory comment that NAMES a forbidden term (e.g.
    a comment explaining why WhatsApp credit-line state must not be touched)
    would fail an assertion that the term is absent -- the test would be
    checking prose, not behaviour.
    """
    import ast

    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)):
            if (node.body and isinstance(node.body[0], ast.Expr)
                    and isinstance(node.body[0].value, ast.Constant)
                    and isinstance(node.body[0].value.value, str)):
                node.body.pop(0)
    return ast.unparse(tree)


def test_deauthorization_service_only_touches_flyer_lady_models():
    """S28: deauthorization MUST NOT delete WABA, phone numbers, System User
    credentials, billing, credit-line state or message history.

    Asserted structurally against the source: the function never references
    any WhatsApp-owned model, which is stronger than asserting on the
    result of one particular call.
    """
    import inspect

    from services import meta_data_deletion

    source = _code_only(inspect.getsource(meta_data_deletion.deauthorize_flyer_lady_user))

    assert "MetaSocialConnection" in source
    assert "MetaSocialOAuthSession" in source
    for whatsapp_model in ("MetaBusinessConnection", "MetaProviderConfig",
                           "waba", "phone_number", "system_user", "credit"):
        assert whatsapp_model not in source, (
            f"deauthorization must not touch WhatsApp-owned {whatsapp_model}"
        )


def test_audit_actions_identify_which_meta_app(clean_meta_env):
    """S29: "The audit trail must tell us WHICH APP caused the event." A
    generic meta_data_deletion_callback cannot."""
    import inspect

    from services import meta_data_deletion

    deletion_source = _code_only(inspect.getsource(meta_data_deletion.delete_meta_user_data))
    deauth_source = _code_only(inspect.getsource(meta_data_deletion.deauthorize_flyer_lady_user))

    assert "meta_flyer_lady_data_deletion" in deletion_source
    assert "meta_flyer_lady_deauthorization" in deauth_source
    # The old capability-agnostic names must be gone.
    assert "meta_user_data_deleted" not in deletion_source


def test_capability_specific_callback_routes_are_registered():
    """S25/S27: each Meta App needs its own callback path."""
    import phanta_app

    rules = {str(rule) for rule in phanta_app.app.url_map.iter_rules()}

    assert "/integrations/meta/flyer-lady/data-deletion" in rules
    assert "/integrations/meta/flyer-lady/deauthorize" in rules
    # The legacy path stays registered: it is already configured in the Meta
    # App Dashboard and removing it would silently break a live callback.
    assert "/data-deletion" in rules


# --------------------------------------------------------------------------
# S31 / S58: no production code may still use the shared config
# --------------------------------------------------------------------------

def test_no_production_code_uses_the_shared_meta_auth_config():
    """S31: after migration, production capability code must not use
    MetaAuthConfig. Only comments, tests and the legacy module itself may
    still mention it."""
    import pathlib

    repo_root = pathlib.Path(__file__).resolve().parents[2]
    offenders = []

    for path in repo_root.rglob("*.py"):
        rel = path.relative_to(repo_root).as_posix()
        if (rel.startswith("tests/") or "__pycache__" in rel
                or rel == "integrations/meta/auth/config.py"):
            continue
        for number, line in enumerate(path.read_text().splitlines(), start=1):
            if "MetaAuthConfig" not in line:
                continue
            stripped = line.strip()
            # Docstring/comment references are allowed; executable ones are not.
            if stripped.startswith("#") or stripped.startswith("*"):
                continue
            if "import" in line or "MetaAuthConfig." in line or ": MetaAuthConfig" in line:
                offenders.append(f"{rel}:{number}: {stripped}")

    assert not offenders, "production code still uses the shared config:\n" + "\n".join(offenders)


def test_flyer_lady_requests_only_its_own_permissions_in_the_oauth_flow():
    """S18/S21/S49: the authorization request must not ask for permissions
    with no implementation behind them."""
    import pathlib

    repo_root = pathlib.Path(__file__).resolve().parents[2]
    source = _code_only((repo_root / "routes/flyer_lady.py").read_text())

    # The old hard-coded over-broad scope string must be gone.
    assert "pages_manage_metadata" not in source
    assert "instagram_basic" not in source
    assert "instagram_content_publish" not in source
    # Scopes now come from the capability config's vetted list.
    assert "config.required_permissions" in source


# --------------------------------------------------------------------------
# S45: database boundaries -- the two capabilities' models must be
# structurally independent of each other.
# --------------------------------------------------------------------------

def test_meta_business_connection_does_not_require_meta_social_connection():
    """S45: WhatsApp's model must not depend on Flyer Lady's."""
    from models.integration_models import MetaBusinessConnection

    columns = MetaBusinessConnection.__table__.columns
    foreign_keys = {fk.target_fullname for column in columns for fk in column.foreign_keys}

    assert not any("meta_social" in target for target in foreign_keys), (
        f"MetaBusinessConnection has a foreign key into Flyer Lady tables: {foreign_keys}"
    )


def test_meta_social_connection_does_not_require_meta_business_connection():
    """S45: Flyer Lady's model must not depend on WhatsApp's."""
    from models.integration_models import MetaSocialConnection

    columns = MetaSocialConnection.__table__.columns
    foreign_keys = {fk.target_fullname for column in columns for fk in column.foreign_keys}

    assert not any("meta_business" in target for target in foreign_keys), (
        f"MetaSocialConnection has a foreign key into WhatsApp tables: {foreign_keys}"
    )


def test_the_two_capability_models_use_separate_tables():
    """S8: preserve the existing separation; do not merge them."""
    from models.integration_models import MetaBusinessConnection, MetaSocialConnection

    assert MetaBusinessConnection.__tablename__ == "meta_business_connections"
    assert MetaSocialConnection.__tablename__ == "meta_social_connections"
    assert MetaBusinessConnection.__tablename__ != MetaSocialConnection.__tablename__


def test_both_capability_models_remain_location_scoped():
    """S45: tenant/location isolation must remain intact for both."""
    from models.integration_models import MetaBusinessConnection, MetaSocialConnection

    for model in (MetaBusinessConnection, MetaSocialConnection):
        assert "location_id" in model.__table__.columns, (
            f"{model.__name__} lost its location scope"
        )


# --------------------------------------------------------------------------
# S55: the deprecated shared config must remain unusable by production
# --------------------------------------------------------------------------

def test_shared_config_module_is_documented_as_deprecated():
    """S31/S55: the module is retained only as an explicitly documented
    backwards-compatibility exception for pre-separation tests."""
    import pathlib

    repo_root = pathlib.Path(__file__).resolve().parents[2]
    source = (repo_root / "integrations/meta/auth/config.py").read_text()

    assert "DEPRECATED" in source
    assert "capability_config" in source


def test_shared_config_constants_are_re_exported_not_duplicated():
    """Prevents the deprecated module and the capability module drifting
    apart: both must resolve to the same objects."""
    from integrations.meta.auth import capability_config, config

    assert config.DEFAULT_GRAPH_API_VERSION is capability_config.DEFAULT_GRAPH_API_VERSION
    assert config.REQUIRED_META_PERMISSIONS is capability_config.WHATSAPP_REQUIRED_PERMISSIONS


def test_whatsapp_permission_registry_uses_whatsapp_permissions():
    """S50: Facebook Page permissions must never appear in the WhatsApp
    App's permission registry."""
    from integrations.meta.auth.permission_registry import REQUIRED_META_PERMISSIONS

    assert not any(p.startswith("pages_") for p in REQUIRED_META_PERMISSIONS)
    assert not any("instagram" in p for p in REQUIRED_META_PERMISSIONS)
    assert "whatsapp_business_messaging" in REQUIRED_META_PERMISSIONS
