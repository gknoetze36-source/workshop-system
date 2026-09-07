import pytest

from integrations.meta.auth.config import MetaAuthConfig
from integrations.meta.services.graph_api_client import GraphApiClient
from services.integration_status import integration_status


def _env(monkeypatch, **values):
    for key in (
        "META_APP_ID", "META_APP_SECRET", "META_GRAPH_API_VERSION",
        "META_SYSTEM_USER_TOKEN", "META_APP_DOMAINS",
        "META_WHATSAPP_CONFIG_ID", "META_EMBEDDED_SIGNUP_CONFIG_ID",
        "META_FLYER_LADY_CONFIG_ID",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def test_shared_meta_config_does_not_require_whatsapp_or_social(monkeypatch):
    _env(monkeypatch, META_APP_ID="123456", META_APP_SECRET="x" * 32)
    config = MetaAuthConfig.from_env()
    assert config.app_id == "123456"
    assert config.embedded_signup_config_id == ""
    assert config.social_config_id == ""


def test_social_requires_only_shared_app_and_social_config(monkeypatch):
    _env(
        monkeypatch,
        META_APP_ID="123456",
        META_APP_SECRET="x" * 32,
        META_FLYER_LADY_CONFIG_ID="987654",
    )
    config = MetaAuthConfig.for_social()
    assert config.social_config_id == "987654"


def test_embedded_signup_requires_its_own_config(monkeypatch):
    _env(monkeypatch, META_APP_ID="123456", META_APP_SECRET="x" * 32)
    with pytest.raises(RuntimeError, match="Embedded Signup"):
        MetaAuthConfig.for_embedded_signup()


def test_system_user_requirement_is_scoped_to_system_user_operations(monkeypatch):
    _env(monkeypatch, META_APP_ID="123456", META_APP_SECRET="x" * 32)
    config = MetaAuthConfig.from_env()
    GraphApiClient(config)
    with pytest.raises(ValueError, match="System User"):
        config.validate_system_user()


def test_flyer_lady_status_does_not_require_whatsapp_system_user(monkeypatch):
    _env(
        monkeypatch,
        META_APP_ID="123456",
        META_APP_SECRET="x" * 32,
        META_FLYER_LADY_CONFIG_ID="987654",
    )
    status = integration_status("flyer_lady")
    assert status["configured"] is True


def test_embedded_signup_status_accepts_alias(monkeypatch):
    _env(
        monkeypatch,
        META_APP_ID="123456",
        META_APP_SECRET="x" * 32,
        META_APP_DOMAINS="https://example.com",
        META_EMBEDDED_SIGNUP_CONFIG_ID="987654",
    )
    status = integration_status("embedded_signup")
    assert status["configured"] is True


def test_phone_number_service_only_requires_shared_config_at_construction(monkeypatch):
    from integrations.meta.whatsapp.phone_number_service import PhoneNumberService

    _env(monkeypatch, META_APP_ID="123456", META_APP_SECRET="x" * 32)
    service = PhoneNumberService()
    assert service.client.config.system_user_token == ""


def test_token_status_service_only_requires_shared_config_for_customer_token_checks(monkeypatch):
    from integrations.meta.services.token_status_service import MetaTokenStatusService

    _env(monkeypatch, META_APP_ID="123456", META_APP_SECRET="x" * 32)
    service = MetaTokenStatusService()
    assert service.config.system_user_token == ""
