"""Instagram API with Instagram Login: client, connect flow, token refresh."""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from integrations.instagram.api_client import InstagramAPIError, InstagramLoginClient
from integrations.instagram.config import InstagramAuthConfig
from tests.unit.test_settings_permission_boundaries import client_with_roles  # noqa: F401  (fixture)

KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


@pytest.fixture
def ig_env(monkeypatch):
    monkeypatch.setenv("META_INSTAGRAM_APP_ID", "555444333")
    monkeypatch.setenv("META_INSTAGRAM_APP_SECRET", "b" * 32)
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", KEY)


class Resp:
    def __init__(self, payload, status=200):
        self.payload, self.status_code, self.ok = payload, status, status < 400

    def json(self):
        return self.payload


class Session:
    def __init__(self, payload, status=200):
        self.payload, self.status = payload, status

    def post(self, url, **kw):
        return Resp(self.payload, self.status)

    get = post


def _client(payload, status=200):
    return InstagramLoginClient(InstagramAuthConfig("1", "s", ""), session=Session(payload, status))


def test_code_exchange_accepts_both_meta_response_shapes():
    nested = {"data": [{"access_token": "short", "user_id": "17", "permissions": "instagram_business_basic"}]}
    assert _client(nested).exchange_code(code="c", redirect_uri="r")["access_token"] == "short"
    assert _client({"access_token": "flat"}).exchange_code(code="c", redirect_uri="r")["access_token"] == "flat"


def test_expired_token_is_reported_as_401_so_flyer_lady_asks_to_reconnect():
    with pytest.raises(InstagramAPIError) as exc:
        _client({"error": {"message": "Session has expired", "code": 190}}, status=400).get_me("t")
    assert exc.value.status_code == 401


def test_connect_start_says_when_instagram_is_not_configured(client_with_roles, monkeypatch):
    monkeypatch.delenv("META_INSTAGRAM_APP_ID", raising=False)
    ctx = client_with_roles
    ctx["login_as"](ctx["owner_email"])
    resp = ctx["client"].get("/dashboard/flyer-lady/instagram/connect", follow_redirects=True)
    assert "META_INSTAGRAM_APP_ID" in resp.get_data(as_text=True)


def test_callback_rejects_a_wrong_state(client_with_roles, ig_env):
    ctx = client_with_roles
    ctx["login_as"](ctx["owner_email"])
    with ctx["client"].session_transaction() as s:
        s["instagram_oauth_state"], s["instagram_oauth_redirect_uri"] = "good", "https://x/cb"
    resp = ctx["client"].get("/dashboard/flyer-lady/instagram/callback?code=c&state=bad", follow_redirects=True)
    assert "connection expired" in resp.get_data(as_text=True)


def test_callback_saves_the_instagram_account_encrypted(client_with_roles, ig_env):
    from database import SessionLocal
    from models.integration_models import InstagramConnection
    from integrations.instagram.tokens import get_token
    ctx = client_with_roles
    ctx["login_as"](ctx["owner_email"])
    with ctx["client"].session_transaction() as s:
        s["instagram_oauth_state"], s["instagram_oauth_redirect_uri"] = "good", "https://x/cb"
    with patch.object(InstagramLoginClient, "exchange_code", return_value={"access_token": "short", "user_id": "17"}), \
         patch.object(InstagramLoginClient, "exchange_for_long_lived_token", return_value={"access_token": "long", "expires_in": 5184000}), \
         patch.object(InstagramLoginClient, "get_me", return_value={"user_id": "1789", "username": "demo_workshop"}):
        resp = ctx["client"].get("/dashboard/flyer-lady/instagram/callback?code=c%23_&state=good", follow_redirects=True)
    assert "@demo_workshop is connected" in resp.get_data(as_text=True)
    db = SessionLocal()
    try:
        row = db.query(InstagramConnection).filter_by(location_id=ctx["location_id"]).one()
        assert (row.instagram_user_id, row.instagram_username, row.connection_status) == ("1789", "demo_workshop", "connected")
        assert row.encrypted_access_token != "long" and get_token(row) == "long"
    finally:
        db.close()


def test_token_refresh_refreshes_week_old_tokens_and_flags_revoked_ones(client_with_roles, ig_env):
    from database import SessionLocal
    from models.integration_models import InstagramConnection
    from integrations.instagram.tokens import get_token, save_token
    from jobs.instagram_token_refresh import run_instagram_token_refresh
    loc = client_with_roles["location_id"]
    now = datetime.now(timezone.utc)
    db = SessionLocal()
    conn = InstagramConnection(location_id=loc, instagram_user_id="1789", encrypted_access_token="")
    save_token(conn, {"access_token": "old", "expires_in": 5184000})
    conn.token_refreshed_at = now - timedelta(days=8)
    db.add(conn); db.commit(); db.close()

    class Fresh:
        def refresh_long_lived_token(self, token):
            assert token == "old"
            return {"access_token": "new", "expires_in": 5184000}

    results = run_instagram_token_refresh(now=now, client=Fresh())
    assert {"location_id": loc, "status": "refreshed"} in results
    db = SessionLocal()
    row = db.query(InstagramConnection).filter_by(location_id=loc).one()
    assert get_token(row) == "new"
    row.token_refreshed_at = now - timedelta(days=8); db.commit(); db.close()

    class Revoked:
        def refresh_long_lived_token(self, token):
            raise InstagramAPIError("expired", status_code=401)

    run_instagram_token_refresh(now=now, client=Revoked())
    db = SessionLocal()
    try:
        assert db.query(InstagramConnection).filter_by(location_id=loc).one().connection_status == "reconnect_required"
    finally:
        db.close()
