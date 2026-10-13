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
