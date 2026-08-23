import pytest
from integrations.meta.auth.config import MetaAuthConfig
from integrations.meta.auth.permission_registry import missing_permissions, validate_permissions
from integrations.meta.auth.system_user_service import SystemUserService
from integrations.meta.services.graph_api_client import GraphApiClient

def cfg():
    return MetaAuthConfig(
        app_id="1234567890123456",
        app_secret="a"*32,
        graph_api_version="v25.0",
        system_user_token="token",
        app_domains=("https://phanta.example",),
        embedded_signup_config_id="123456",
    )

def test_meta_auth_config_validation():
    c=cfg(); c.validate()
    assert c.graph_base_url()=="https://graph.facebook.com/v25.0"

def test_https_domain_required():
    with pytest.raises(RuntimeError):
        MetaAuthConfig.from_env()

def test_required_permissions():
    assert missing_permissions({"business_management"}) == [
        "whatsapp_business_management",
        "whatsapp_business_messaging",
    ]
    assert all(validate_permissions({"whatsapp_business_messaging","whatsapp_business_management","business_management"}).values())

def test_graph_client_uses_system_user_token(monkeypatch):
    class FakeResponse:
        ok=True
        def json(self): return {"id":"sys_123","name":"PHANTA System User"}
    class FakeSession:
        def request(self, method, url, **kwargs):
            assert kwargs["headers"]["Authorization"] == "Bearer token"
            assert url == "https://graph.facebook.com/v25.0/me"
            return FakeResponse()
    client=GraphApiClient(cfg(), FakeSession())
    assert client.get("/me", params={"fields":"id,name"})["id"]=="sys_123"

def test_system_user_health_check():
    class FakeClient:
        def get(self, path, params=None):
            return {"id":"sys_123","name":"PHANTA"}
    result=SystemUserService(cfg(), FakeClient()).health_check()
    assert result == {"healthy":True,"system_user_id":"sys_123","name":"PHANTA"}

def test_graph_version_validation():
    bad = cfg().__class__(
        app_id=cfg().app_id,
        app_secret=cfg().app_secret,
        graph_api_version="25.0",
        system_user_token=cfg().system_user_token,
        app_domains=cfg().app_domains,
    )
    with pytest.raises(ValueError, match="META_GRAPH_API_VERSION"):
        bad.validate()


def test_meta_domain_must_be_clean_https_url():
    bad = cfg().__class__(
        app_id=cfg().app_id,
        app_secret=cfg().app_secret,
        graph_api_version=cfg().graph_api_version,
        system_user_token=cfg().system_user_token,
        app_domains=("http://phanta.example",),
    )
    with pytest.raises(ValueError, match="HTTPS"):
        bad.validate()


def test_public_oauth_configuration_does_not_expose_secrets():
    from integrations.meta.auth.oauth_client import MetaOAuthClient
    public = MetaOAuthClient(cfg()).public_configuration()
    assert public.app_id == cfg().app_id
    assert not hasattr(public, "app_secret")
    assert not hasattr(public, "system_user_token")
