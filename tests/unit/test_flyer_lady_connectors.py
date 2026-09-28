"""flyer_lady/connectors/ -- the new interface/protocol layer.

These tests are not about whether Facebook/Instagram/Google publishing
works -- that's already covered by test_flyer_lady.py,
test_flyer_lady_retry_policy.py, test_google_business*.py, and the
others. These are specifically about the connector layer's one real
job: proving each method genuinely calls the existing implementation
it claims to, rather than doing something new or silently no-op'ing.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from flyer_lady.connectors import FlyerLadyConnector, get_connector, registered_platforms
from flyer_lady.connectors.meta_social import FacebookConnector, InstagramConnector
from flyer_lady.connectors.google_business import GoogleBusinessConnector


def test_exactly_six_platforms_are_registered():
    """facebook, instagram, google_business, x, threads, and
    (added deliberately, private/beta) tiktok."""
    assert registered_platforms() == ["facebook", "google_business", "instagram", "threads", "tiktok", "x"]


def test_get_connector_returns_the_right_type_for_each_platform():
    assert isinstance(get_connector("facebook"), FacebookConnector)
    assert isinstance(get_connector("instagram"), InstagramConnector)
    assert isinstance(get_connector("google_business"), GoogleBusinessConnector)


def test_unregistered_platform_raises_a_clear_error():
    with pytest.raises(KeyError, match="not-a-real-platform"):
        get_connector("not-a-real-platform")


@pytest.mark.parametrize("platform", ["facebook", "instagram", "google_business"])
def test_every_connector_conforms_to_the_protocol(platform):
    connector = get_connector(platform)
    assert isinstance(connector, FlyerLadyConnector)
    for method in ("authorize_url", "exchange_code", "discover_accounts", "refresh",
                   "revoke", "health", "capabilities", "publish_step", "fetch_metrics"):
        assert callable(getattr(connector, method))


# --------------------------------------------------------------------
# Facebook / Instagram -- authorize_url must match the real route exactly
# --------------------------------------------------------------------

def test_facebook_authorize_url_matches_the_real_route(monkeypatch):
    """The actual proof, not an assumption: builds the URL the same way
    routes/flyer_lady.py's social_connect_start() does, using the same
    config, and compares them directly rather than just checking it
    looks plausible."""
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "123456789012345")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "a" * 32)
    monkeypatch.setenv("META_FLYER_LADY_CONFIG_ID", "999888777")
    monkeypatch.setenv("META_FLYER_LADY_GRAPH_API_VERSION", "v26.0")

    from integrations.meta.auth.capability_config import FlyerLadyMetaConfig
    config = FlyerLadyMetaConfig.from_env()

    from urllib.parse import urlencode
    import os
    scopes = os.getenv("META_SOCIAL_OAUTH_SCOPES", ",".join(config.required_permissions))
    expected_params = urlencode({
        "client_id": config.app_id, "config_id": config.facebook_login_config_id,
        "redirect_uri": "https://app.example.test/callback", "state": "abc123",
        "scope": scopes, "response_type": "code",
    })
    expected_url = f"https://www.facebook.com/{config.graph_api_version}/dialog/oauth?{expected_params}"

    connector = FacebookConnector()
    actual_url = connector.authorize_url(redirect_uri="https://app.example.test/callback", state="abc123")

    assert actual_url == expected_url


def test_instagram_and_facebook_share_the_same_authorize_url(monkeypatch):
    """There is one Meta OAuth flow, not two -- Instagram's connector
    must build the identical URL Facebook's does, since they share the
    same underlying connection."""
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "123456789012345")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "a" * 32)
    monkeypatch.setenv("META_FLYER_LADY_CONFIG_ID", "999888777")
    fb = FacebookConnector()
    ig = InstagramConnector()
    assert fb.authorize_url(redirect_uri="https://x.test/cb", state="s1") == \
        ig.authorize_url(redirect_uri="https://x.test/cb", state="s1")


def test_facebook_discover_accounts_calls_the_existing_list_pages(monkeypatch):
    """Not just 'returns something' -- proves the real existing method
    was actually invoked, with the real token, not bypassed."""
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "123456789012345")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "a" * 32)

    fake_pages = {"data": [{"id": "page-1", "name": "Test Page", "access_token": "pt"}]}
    with patch(
        "flyer_lady.connectors.meta_social.MetaSocialGraphClient.list_pages",
        return_value=fake_pages,
    ) as mock_list_pages:
        result = FacebookConnector().discover_accounts(token="user-token-xyz")

    mock_list_pages.assert_called_once_with("user-token-xyz")
    assert result == fake_pages["data"]


def test_instagram_discover_accounts_filters_to_pages_with_linked_instagram(monkeypatch):
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "123456789012345")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "a" * 32)

    fake_pages = {"data": [
        {"id": "page-1", "name": "Has IG", "instagram_business_account": {"id": "ig-1"}},
        {"id": "page-2", "name": "No IG"},
    ]}
    with patch("flyer_lady.connectors.meta_social.MetaSocialGraphClient.list_pages", return_value=fake_pages):
        result = InstagramConnector().discover_accounts(token="user-token-xyz")

    assert len(result) == 1
    assert result[0]["id"] == "page-1"


def test_facebook_refresh_is_honestly_not_implemented():
    """Must not silently succeed or fabricate a token -- there is no
    existing VANTA refresh flow for Meta Page tokens."""
    with pytest.raises(NotImplementedError):
        FacebookConnector().refresh(refresh_token="whatever")


def _meta_session(monkeypatch):
    """A real in-memory session with a real MetaSocialConnection row --
    revoke() now needs to look one up and decrypt its token, which a
    bare MagicMock can't meaningfully stand in for."""
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    # _cfg() falls back to FlyerLadyMetaConfig.from_env() when no config
    # is passed explicitly, which requires these -- same values every
    # other test in this file already uses.
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "123456789012345")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "a" * 32)
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from models.core import Base, Location, Owner
    from models.integration_models import MetaSocialConnection
    from integrations.meta.auth.token_store import MetaTokenStore

    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    from models import integration_models  # noqa: F401
    Base.metadata.create_all(engine)
    session = Session(engine)
    location = Location(owner=Owner(), name="Revoke Test Workshop")
    session.add(location)
    session.flush()
    connection = MetaSocialConnection(
        location_id=location.id, meta_user_id="meta-user-1", page_id="page-1",
        encrypted_page_access_token="", connection_status="connected",
    )
    session.add(connection)
    session.flush()
    MetaTokenStore().save_social_token(session, connection, "the-page-token")
    session.flush()
    return session


def test_facebook_revoke_calls_meta_before_the_existing_deauthorize_function():
    """DEFECT-006 repair: revoke() previously only did the local half --
    a workshop that "disconnected" Facebook/Instagram had its VANTA-side
    record marked revoked, but the token stayed live and usable on
    Meta's own side. Now it calls Meta's DELETE /{user-id}/permissions
    first (best-effort, matching XConnector's own pattern), then the
    unchanged local deauthorize_flyer_lady_user()."""
    session = _meta_session(pytest.MonkeyPatch())
    with patch(
        "flyer_lady.connectors.meta_social.GraphApiClient.delete_with_token",
        return_value={},
    ) as mock_delete, patch(
        "flyer_lady.connectors.meta_social.deauthorize_flyer_lady_user",
        return_value={"user_found": True, "connections_revoked": 1},
    ) as mock_deauth:
        result = FacebookConnector().revoke(session, meta_user_id="meta-user-1")

    mock_delete.assert_called_once_with("the-page-token", "/meta-user-1/permissions")
    mock_deauth.assert_called_once_with(session, "meta-user-1")
    assert result == {"user_found": True, "connections_revoked": 1}


def test_facebook_revoke_still_deauthorizes_locally_even_if_meta_is_unreachable():
    """Best-effort courtesy call, same as XConnector.revoke(): if Meta's
    API is unreachable, the local revoke -- the part that actually stops
    this location from publishing further -- must still happen."""
    session = _meta_session(pytest.MonkeyPatch())
    with patch(
        "flyer_lady.connectors.meta_social.GraphApiClient.delete_with_token",
        side_effect=RuntimeError("Meta unreachable"),
    ), patch(
        "flyer_lady.connectors.meta_social.deauthorize_flyer_lady_user",
        return_value={"user_found": True, "connections_revoked": 1},
    ) as mock_deauth:
        result = FacebookConnector().revoke(session, meta_user_id="meta-user-1")

    mock_deauth.assert_called_once_with(session, "meta-user-1")
    assert result == {"user_found": True, "connections_revoked": 1}


def test_facebook_revoke_with_no_local_connection_still_deauthorizes():
    """A meta_user_id with no matching MetaSocialConnection row (already
    disconnected, or a deauthorization callback for an unknown user) must
    not error -- deauthorize_flyer_lady_user() is itself already
    idempotent for this case."""
    session = _meta_session(pytest.MonkeyPatch())
    with patch(
        "flyer_lady.connectors.meta_social.GraphApiClient.delete_with_token",
    ) as mock_delete, patch(
        "flyer_lady.connectors.meta_social.deauthorize_flyer_lady_user",
        return_value={"user_found": False, "connections_revoked": 0},
    ) as mock_deauth:
        result = FacebookConnector().revoke(session, meta_user_id="some-other-user")

    mock_delete.assert_not_called()
    mock_deauth.assert_called_once_with(session, "some-other-user")
    assert result == {"user_found": False, "connections_revoked": 0}


def test_facebook_capabilities_match_the_real_platform_strings():
    """These must be exactly what flyer_lady/publish_service.py
    actually dispatches on -- checked directly against that source of
    truth rather than duplicated by hand."""
    import inspect
    from flyer_lady import publish_service
    source = inspect.getsource(publish_service)
    caps = FacebookConnector().capabilities()
    for post_type in caps["post_types"]:
        assert f'"{post_type}"' in source or f"'{post_type}'" in source


def test_instagram_capabilities_match_the_real_platform_strings():
    import inspect
    from flyer_lady import publish_service
    source = inspect.getsource(publish_service)
    caps = InstagramConnector().capabilities()
    for post_type in caps["post_types"]:
        assert post_type in source


# --------------------------------------------------------------------
# Google Business
# --------------------------------------------------------------------

def test_google_authorize_url_matches_the_real_route(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id.apps.googleusercontent.com")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-secret")

    from integrations.google.auth.config import GoogleAuthConfig
    from integrations.google.business.api_client import GoogleBusinessApiClient
    from urllib.parse import urlencode

    config = GoogleAuthConfig.from_env()
    expected_params = urlencode({
        "client_id": config.client_id, "redirect_uri": "https://app.example.test/cb",
        "response_type": "code", "access_type": "offline", "prompt": "consent",
        "scope": GoogleBusinessApiClient.SCOPE, "state": "s1",
    })
    expected_url = f"https://accounts.google.com/o/oauth2/v2/auth?{expected_params}"

    actual_url = GoogleBusinessConnector().authorize_url(redirect_uri="https://app.example.test/cb", state="s1")
    assert actual_url == expected_url


def test_google_exchange_code_calls_the_existing_client_method(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-secret")

    with patch(
        "flyer_lady.connectors.google_business.GoogleBusinessApiClient.exchange_code_for_tokens",
        return_value={"access_token": "at", "refresh_token": "rt"},
    ) as mock_exchange:
        result = GoogleBusinessConnector().exchange_code(code="auth-code", redirect_uri="https://x.test/cb")

    mock_exchange.assert_called_once_with("auth-code", "https://x.test/cb")
    assert result == {"access_token": "at", "refresh_token": "rt"}


def test_google_refresh_calls_the_existing_client_method(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-secret")

    with patch(
        "flyer_lady.connectors.google_business.GoogleBusinessApiClient.refresh_access_token",
        return_value="new-access-token",
    ) as mock_refresh:
        result = GoogleBusinessConnector().refresh(refresh_token="stored-refresh-token")

    mock_refresh.assert_called_once_with("stored-refresh-token")
    assert result == "new-access-token"


def test_google_discover_accounts_calls_list_accounts_and_list_locations(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-secret")

    with patch(
        "flyer_lady.connectors.google_business.GoogleBusinessApiClient.list_accounts",
        return_value=[{"name": "accounts/1"}],
    ) as mock_accounts, patch(
        "flyer_lady.connectors.google_business.GoogleBusinessApiClient.list_locations",
        return_value=[{"name": "accounts/1/locations/2", "title": "Test Location"}],
    ) as mock_locations:
        result = GoogleBusinessConnector().discover_accounts(token="google-token")

    mock_accounts.assert_called_once_with("google-token")
    mock_locations.assert_called_once_with("google-token", "accounts/1")
    assert result == [{"account_id": "accounts/1", "name": "accounts/1/locations/2", "title": "Test Location"}]


def test_google_capabilities_matches_the_real_platform_string():
    import inspect
    from flyer_lady import publish_service
    source = inspect.getsource(publish_service)
    caps = GoogleBusinessConnector().capabilities()
    assert caps["post_types"] == ["google_business_post"]
    assert '"google_business_post"' in source


def test_google_revoke_marks_the_real_connection_revoked():
    """The one method with genuinely new (if minimal) logic -- proves
    it actually mutates the real GoogleBusinessConnection row, scoped
    to the given location, and nothing else."""
    from database import get_session
    from models.integration_models import GoogleBusinessConnection
    import re, phanta_app

    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html)
        return m.group(1) if m else None

    email = "connectorrevoke@test.example"
    token = csrf_from("/register")
    client.post("/register", data={
        "full_name": "Test", "email": email, "password": "SuperSecret123",
        "confirm_password": "SuperSecret123", "csrf_token": token,
    })
    token2 = csrf_from("/onboarding/location")
    client.post("/onboarding/location", data={
        "location_name": "Connector Revoke Check", "industry": "workshop", "csrf_token": token2,
    })

    from database import query_db
    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]

    db = get_session()
    try:
        connection = GoogleBusinessConnection(
            location_id=location_id, google_account_id="accounts/1", google_location_id="accounts/1/locations/2",
            business_name="Test", connection_status="connected", encrypted_refresh_token="ignored",
        )
        db.add(connection)
        db.commit()

        result = GoogleBusinessConnector().revoke(db, location_id=location_id)
        db.commit()
    finally:
        db.close()

    assert result == {"connection_found": True, "connection_status": "revoked"}
    row = query_db("SELECT connection_status FROM google_business_connections WHERE location_id=%s", (location_id,), one=True)
    assert row["connection_status"] == "revoked"


def test_google_fetch_metrics_is_honestly_not_implemented():
    with pytest.raises(NotImplementedError):
        GoogleBusinessConnector().fetch_metrics(session=None, location_id=1)


def test_facebook_fetch_metrics_is_honestly_not_implemented():
    with pytest.raises(NotImplementedError):
        FacebookConnector().fetch_metrics(session=None, location_id=1)


# --------------------------------------------------------------------
# publish_step -- must call the real, unchanged publish service
# --------------------------------------------------------------------

@pytest.mark.parametrize("connector_cls", [FacebookConnector, InstagramConnector, GoogleBusinessConnector])
def test_publish_step_calls_the_real_publish_service(connector_cls):
    """The core "do not duplicate publishers" proof: every connector's
    publish_step must call the exact same, unchanged
    FlyerLadyPublishService.publish_post() -- not a connector-specific
    reimplementation."""
    fake_post = object()
    with patch(
        "flyer_lady.publish_service.FlyerLadyPublishService.publish_post",
        return_value="fake-result",
    ) as mock_publish:
        result = connector_cls().publish_step(session="fake-session", location_id=42, post=fake_post)

    mock_publish.assert_called_once_with("fake-session", 42, fake_post)
    assert result == "fake-result"
