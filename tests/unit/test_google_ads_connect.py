"""Google Ads OAuth connect flow (routes/google_ads.py)."""
import re
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from database import query_db

CALLBACK = "https://app.vantaautomations.co.za/dashboard/google-ads/connect/callback"


def _client(monkeypatch, suffix):
    import phanta_app
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setenv("GOOGLE_REDIRECT_URI", CALLBACK)
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    email = f"googleads-{suffix}@test.example"

    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html) or re.search(r'name="csrf-token" content="([^"]+)"', html)
        return m.group(1) if m else None

    client.post("/register", data={"full_name": "Test", "email": email, "password": "SuperSecret123",
                                   "confirm_password": "SuperSecret123", "csrf_token": csrf_from("/register")})
    client.post("/onboarding/location", data={"location_name": f"Google Ads Workshop {suffix}", "industry": "workshop",
                                              "csrf_token": csrf_from("/onboarding/location")})
    location_id = query_db("SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s", (email,), one=True)["id"]
    return client, location_id


def _start(client):
    response = client.get("/dashboard/google-ads/connect/start", follow_redirects=False)
    assert response.status_code == 302
    return parse_qs(urlparse(response.headers["Location"]).query)


def test_start_sends_the_registered_callback_and_adwords_scope(monkeypatch):
    client, _ = _client(monkeypatch, "start")
    params = _start(client)
    assert params["redirect_uri"] == [CALLBACK]
    assert params["scope"] == ["https://www.googleapis.com/auth/adwords"]
    assert params["access_type"] == ["offline"]
    assert params["state"][0]


def test_callback_saves_an_encrypted_refresh_token(monkeypatch):
    client, location_id = _client(monkeypatch, "save")
    state = _start(client)["state"][0]
    with patch("routes.google_ads.GoogleAdsApiClient") as mock_cls:
        mock_cls.return_value.exchange_code_for_tokens.return_value = {"access_token": "a", "refresh_token": "real-refresh"}
        mock_cls.return_value.list_accessible_customers.return_value = ["1234567890"]
        response = client.get(f"/dashboard/google-ads/connect/callback?code=c&state={state}")
    assert response.status_code == 302
    mock_cls.return_value.exchange_code_for_tokens.assert_called_once_with("c", CALLBACK)
    row = query_db("SELECT encrypted_refresh_token, connection_status FROM google_ads_connections WHERE location_id=%s", (location_id,), one=True)
    assert row["connection_status"] == "connected"
    assert "real-refresh" not in row["encrypted_refresh_token"]
    from integrations.google.auth.token_store import GoogleTokenStore
    assert GoogleTokenStore().decrypt(row["encrypted_refresh_token"]) == "real-refresh"


def test_callback_rejects_a_forged_state(monkeypatch):
    client, location_id = _client(monkeypatch, "forged")
    _start(client)
    with patch("routes.google_ads.GoogleAdsApiClient") as mock_cls:
        client.get("/dashboard/google-ads/connect/callback?code=c&state=forged")
    mock_cls.return_value.exchange_code_for_tokens.assert_not_called()
    assert query_db("SELECT id FROM google_ads_connections WHERE location_id=%s", (location_id,), one=True) is None


def test_callback_without_a_refresh_token_saves_nothing(monkeypatch):
    client, location_id = _client(monkeypatch, "norefresh")
    state = _start(client)["state"][0]
    with patch("routes.google_ads.GoogleAdsApiClient") as mock_cls:
        mock_cls.return_value.exchange_code_for_tokens.return_value = {"access_token": "a"}
        client.get(f"/dashboard/google-ads/connect/callback?code=c&state={state}")
    assert query_db("SELECT id FROM google_ads_connections WHERE location_id=%s", (location_id,), one=True) is None


def test_old_google_business_routes_are_gone(monkeypatch):
    client, _ = _client(monkeypatch, "gone")
    assert client.get("/dashboard/google-business/connect/start").status_code == 404


def test_callback_picks_the_only_ads_account(monkeypatch):
    monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-token")
    client, location_id = _client(monkeypatch, "oneacct")
    state = _start(client)["state"][0]
    with patch("routes.google_ads.GoogleAdsApiClient") as mock_cls:
        inst = mock_cls.return_value
        inst.developer_token = "dev-token"
        inst.exchange_code_for_tokens.return_value = {"access_token": "a", "refresh_token": "r"}
        inst.list_accessible_customers.return_value = ["1234567890"]
        client.get(f"/dashboard/google-ads/connect/callback?code=c&state={state}")
    assert query_db("SELECT customer_id FROM google_ads_connections WHERE location_id=%s", (location_id,), one=True)["customer_id"] == "1234567890"


def test_callback_with_several_accounts_only_accepts_one_google_returned(monkeypatch):
    monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-token")
    client, location_id = _client(monkeypatch, "manyacct")
    state = _start(client)["state"][0]
    with patch("routes.google_ads.GoogleAdsApiClient") as mock_cls:
        inst = mock_cls.return_value
        inst.developer_token = "dev-token"
        inst.exchange_code_for_tokens.return_value = {"access_token": "a", "refresh_token": "r"}
        inst.list_accessible_customers.return_value = ["1111111111", "2222222222"]
        page = client.get(f"/dashboard/google-ads/connect/callback?code=c&state={state}").get_data(as_text=True)
    assert "222-222-2222" in page
    token = re.search(r'name="csrf_token" value="([^"]+)"', page).group(1)
    client.post("/dashboard/google-ads/connect/account", data={"customer_id": "9999999999", "csrf_token": token})
    assert query_db("SELECT customer_id FROM google_ads_connections WHERE location_id=%s", (location_id,), one=True)["customer_id"] is None


def test_no_developer_token_needed_since_google_sunset_them(monkeypatch):
    """Google sunset developer tokens on 9 Sep 2026: calls must work without one,
    and a Test-access Cloud project gets a plain-English fix."""
    from integrations.google.ads.api_client import GoogleAdsAPIError, GoogleAdsApiClient
    from integrations.google.auth.config import GoogleAuthConfig
    monkeypatch.delenv("GOOGLE_ADS_DEVELOPER_TOKEN", raising=False)
    sent = {}

    class Resp:
        def __init__(self, status, payload, text=""):
            self.status_code, self.ok, self._p, self.text = status, status < 400, payload, text
        def json(self):
            return self._p

    class S:
        def __init__(self, resp):
            self.resp = resp
        def get(self, url, headers=None, timeout=None):
            sent.update(headers); return self.resp

    ok = GoogleAdsApiClient(GoogleAuthConfig("id", "secret", ""), session=S(Resp(200, {"resourceNames": ["customers/1234567890"]})))
    assert ok.list_accessible_customers("tok") == ["1234567890"]
    assert "developer-token" not in sent

    denied = GoogleAdsApiClient(GoogleAuthConfig("id", "secret", ""), session=S(Resp(403, {"error": {"message": "denied"}}, text="CLOUD_PROJECT_NOT_APPROVED_FOR_PRODUCTION")))
    import pytest
    with pytest.raises(GoogleAdsAPIError, match="Explorer access"):
        denied.list_accessible_customers("tok")
