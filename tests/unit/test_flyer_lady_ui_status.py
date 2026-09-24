"""flyer_lady/connectors/ui_status.py -- the six UI-facing connector
states and the publish selector's actual gate.

Deliberately separate from test_flyer_lady_connector_status.py, which
covers status.py's own four-value contract (connected,
reconnect_required, disconnected, pending_review) for a different,
already-delivered task. These tests are about the six-label UI mapping
specifically: the real distinction between "never connected" (Connect)
and "revoked" (Disconnected) that status.py intentionally collapses,
TikTok's Private beta label never masking a genuine Reconnect/
Disconnected problem, and the exact AND the publish selector requires
-- status == connected AND the required capability is available.
"""
from __future__ import annotations

import re

import pytest

from flyer_lady.connectors.ui_status import (
    PRIVATE_BETA_PLATFORMS,
    UI_AWAITING_APPROVAL,
    UI_CONNECT,
    UI_CONNECTED,
    UI_DISCONNECTED,
    UI_PRIVATE_BETA,
    UI_RECONNECT,
    UI_STATES,
    get_all_connector_ui_states,
    get_connector_ui_state,
)


def _register_and_onboard(client, suffix):
    email = f"uistatus-{suffix}@test.example"

    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html)
        return m.group(1) if m else None

    token = csrf_from("/register")
    client.post("/register", data={
        "full_name": "Test", "email": email, "password": "SuperSecret123",
        "confirm_password": "SuperSecret123", "csrf_token": token,
    })
    token2 = csrf_from("/onboarding/location")
    client.post("/onboarding/location", data={
        "location_name": f"UI Status {suffix}", "industry": "workshop", "csrf_token": token2,
    })
    from database import query_db
    return query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]


@pytest.fixture
def client():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    return phanta_app.app.test_client()


def test_exactly_six_ui_states_exist():
    assert UI_STATES == {"Connected", "Connect", "Reconnect", "Disconnected", "Awaiting approval", "Private beta"}


def test_undeployed_platform_shows_connect_and_is_not_selectable(client):
    """No META_FLYER_LADY_* etc set at all -- must show Connect, never
    Connected, and must never be offered for publishing."""
    location_id = _register_and_onboard(client, "undeployed")
    from database import get_session
    db = get_session()
    try:
        result = get_connector_ui_state(db, location_id, "facebook")
    finally:
        db.close()
    assert result["ui_status"] == UI_CONNECT
    assert result["selectable_for_publish"] is False


def test_never_connected_shows_connect_not_disconnected(client, monkeypatch):
    """The core distinction this module exists for: a platform with no
    connection row at all reads as Connect, not Disconnected --
    status.py's normalize_status() intentionally collapses both into
    one "disconnected" bucket for its own four-value contract, but the
    UI needs them told apart."""
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "1")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "2")
    monkeypatch.setenv("META_FLYER_LADY_CONFIG_ID", "3")
    location_id = _register_and_onboard(client, "neverconnected")
    from database import get_session
    db = get_session()
    try:
        result = get_connector_ui_state(db, location_id, "facebook")
    finally:
        db.close()
    assert result["ui_status"] == UI_CONNECT


def test_revoked_shows_disconnected_not_connect(client, monkeypatch):
    """The other half of that same distinction: a connection that
    existed and was revoked must read as Disconnected, not Connect --
    telling a workshop "you had this connected and it's gone" is
    different information from "you've never set this up"."""
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "1")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "2")
    monkeypatch.setenv("META_FLYER_LADY_CONFIG_ID", "3")
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    location_id = _register_and_onboard(client, "revoked")

    from database import get_session
    from models.integration_models import MetaSocialConnection
    from integrations.meta.auth.token_store import MetaTokenStore

    db = get_session()
    try:
        connection = MetaSocialConnection(
            location_id=location_id, meta_user_id="m1", page_id="p1", page_name="Page",
            connection_status="connected", encrypted_page_access_token="",
        )
        db.add(connection)
        db.flush()
        MetaTokenStore().save_social_token(db, connection, "tok")
        connection.connection_status = "revoked"
        db.commit()

        result = get_connector_ui_state(db, location_id, "facebook")
    finally:
        db.close()

    assert result["ui_status"] == UI_DISCONNECTED
    assert result["selectable_for_publish"] is False


def test_connected_and_capable_is_selectable(client, monkeypatch):
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "1")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "2")
    monkeypatch.setenv("META_FLYER_LADY_CONFIG_ID", "3")
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    location_id = _register_and_onboard(client, "readyfb")

    from database import get_session
    from models.integration_models import MetaSocialConnection
    from integrations.meta.auth.token_store import MetaTokenStore

    db = get_session()
    try:
        connection = MetaSocialConnection(
            location_id=location_id, meta_user_id="m1", page_id="p1", page_name="Page",
            connection_status="connected", encrypted_page_access_token="",
        )
        db.add(connection)
        db.flush()
        MetaTokenStore().save_social_token(db, connection, "tok")
        db.commit()

        result = get_connector_ui_state(db, location_id, "facebook")
    finally:
        db.close()

    assert result["ui_status"] == UI_CONNECTED
    assert result["selectable_for_publish"] is True
    assert result["capabilities"]["post_types"] == ["facebook_feed", "facebook_story"]


def test_instagram_connected_but_no_linked_account_is_not_selectable(client, monkeypatch):
    """The exact scenario this task's second requirement guards
    against: Facebook's OAuth genuinely succeeded (connection_status=
    connected), but the connected Page has no linked Instagram
    Business Account -- publish_service.py's own instagram branch
    would raise "Instagram Business Account is not connected" on the
    very next attempt. The selector must exclude it before that ever
    happens."""
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "1")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "2")
    monkeypatch.setenv("META_FLYER_LADY_CONFIG_ID", "3")
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    location_id = _register_and_onboard(client, "ignolink")

    from database import get_session
    from models.integration_models import MetaSocialConnection
    from integrations.meta.auth.token_store import MetaTokenStore

    db = get_session()
    try:
        connection = MetaSocialConnection(
            location_id=location_id, meta_user_id="m1", page_id="p1", page_name="Page",
            connection_status="connected", encrypted_page_access_token="",
        )
        db.add(connection)
        db.flush()
        MetaTokenStore().save_social_token(db, connection, "tok")
        db.commit()

        result = get_connector_ui_state(db, location_id, "instagram")
    finally:
        db.close()

    assert result["selectable_for_publish"] is False


def test_instagram_with_linked_account_is_selectable(client, monkeypatch):
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "1")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "2")
    monkeypatch.setenv("META_FLYER_LADY_CONFIG_ID", "3")
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    location_id = _register_and_onboard(client, "iglinked")

    from database import get_session
    from models.integration_models import MetaSocialConnection
    from integrations.meta.auth.token_store import MetaTokenStore

    db = get_session()
    try:
        connection = MetaSocialConnection(
            location_id=location_id, meta_user_id="m1", page_id="p1", page_name="Page",
            connection_status="connected", instagram_business_account_id="ig-1",
            encrypted_page_access_token="",
        )
        db.add(connection)
        db.flush()
        MetaTokenStore().save_social_token(db, connection, "tok")
        db.commit()

        result = get_connector_ui_state(db, location_id, "instagram")
    finally:
        db.close()

    assert result["ui_status"] == UI_CONNECTED
    assert result["selectable_for_publish"] is True


def test_reconnect_required_shows_reconnect(client, monkeypatch):
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "1")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "2")
    monkeypatch.setenv("META_FLYER_LADY_CONFIG_ID", "3")
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    location_id = _register_and_onboard(client, "reconnectneeded")

    from database import get_session
    from models.integration_models import MetaSocialConnection
    from integrations.meta.auth.token_store import MetaTokenStore

    db = get_session()
    try:
        connection = MetaSocialConnection(
            location_id=location_id, meta_user_id="m1", page_id="p1", page_name="Page",
            connection_status="connected", encrypted_page_access_token="",
        )
        db.add(connection)
        db.flush()
        MetaTokenStore().save_social_token(db, connection, "tok")
        connection.connection_status = "reconnect_required"
        db.commit()

        result = get_connector_ui_state(db, location_id, "facebook")
    finally:
        db.close()

    assert result["ui_status"] == UI_RECONNECT
    assert result["selectable_for_publish"] is False


# ======================================================================
# TikTok's Private beta label
# ======================================================================

def test_tiktok_is_in_the_private_beta_set():
    assert "tiktok" in PRIVATE_BETA_PLATFORMS


def test_tiktok_shows_private_beta_when_not_yet_connected(client, monkeypatch):
    monkeypatch.setenv("TIKTOK_CLIENT_KEY", "k")
    monkeypatch.setenv("TIKTOK_CLIENT_SECRET", "s")
    location_id = _register_and_onboard(client, "tiktoknotyet")
    from database import get_session
    db = get_session()
    try:
        result = get_connector_ui_state(db, location_id, "tiktok")
    finally:
        db.close()
    assert result["ui_status"] == UI_PRIVATE_BETA
    assert result["selectable_for_publish"] is False


def test_tiktok_shows_private_beta_and_is_selectable_once_fully_ready(client, monkeypatch):
    """Private beta does not mean unusable: once genuinely connected
    with a confirmed privacy level, TikTok must be selectable -- the
    label describes the SELF_ONLY restriction, it is not a block."""
    monkeypatch.setenv("TIKTOK_CLIENT_KEY", "k")
    monkeypatch.setenv("TIKTOK_CLIENT_SECRET", "s")
    monkeypatch.setenv("TIKTOK_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    location_id = _register_and_onboard(client, "tiktokready")

    from database import get_session
    from models.integration_models import TikTokConnection
    from integrations.tiktok.auth.token_store import TikTokTokenStore

    db = get_session()
    try:
        connection = TikTokConnection(
            location_id=location_id, tiktok_open_id="o1", connection_status="connected",
            encrypted_access_token="", encrypted_refresh_token="", selected_privacy_level="SELF_ONLY",
        )
        db.add(connection)
        db.flush()
        TikTokTokenStore().save_connection_tokens(db, connection, access_token="a", refresh_token="r")
        db.commit()

        result = get_connector_ui_state(db, location_id, "tiktok")
    finally:
        db.close()

    assert result["ui_status"] == UI_PRIVATE_BETA
    assert result["selectable_for_publish"] is True


def test_tiktok_private_beta_does_not_mask_reconnect_required(client, monkeypatch):
    """The requirement that actually matters: a genuine auth problem
    must still surface as Reconnect, not be hidden behind the
    permanent Private beta label."""
    monkeypatch.setenv("TIKTOK_CLIENT_KEY", "k")
    monkeypatch.setenv("TIKTOK_CLIENT_SECRET", "s")
    monkeypatch.setenv("TIKTOK_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    location_id = _register_and_onboard(client, "tiktokbroken")

    from database import get_session
    from models.integration_models import TikTokConnection
    from integrations.tiktok.auth.token_store import TikTokTokenStore

    db = get_session()
    try:
        connection = TikTokConnection(
            location_id=location_id, tiktok_open_id="o1", connection_status="connected",
            encrypted_access_token="", encrypted_refresh_token="", selected_privacy_level="SELF_ONLY",
        )
        db.add(connection)
        db.flush()
        TikTokTokenStore().save_connection_tokens(db, connection, access_token="a", refresh_token="r")
        connection.connection_status = "reconnect_required"
        db.commit()

        result = get_connector_ui_state(db, location_id, "tiktok")
    finally:
        db.close()

    assert result["ui_status"] == UI_RECONNECT
    assert result["selectable_for_publish"] is False


def test_tiktok_private_beta_does_not_mask_disconnected(client, monkeypatch):
    monkeypatch.setenv("TIKTOK_CLIENT_KEY", "k")
    monkeypatch.setenv("TIKTOK_CLIENT_SECRET", "s")
    monkeypatch.setenv("TIKTOK_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    location_id = _register_and_onboard(client, "tiktokrevoked")

    from database import get_session
    from models.integration_models import TikTokConnection
    from integrations.tiktok.auth.token_store import TikTokTokenStore

    db = get_session()
    try:
        connection = TikTokConnection(
            location_id=location_id, tiktok_open_id="o1", connection_status="connected",
            encrypted_access_token="", encrypted_refresh_token="",
        )
        db.add(connection)
        db.flush()
        TikTokTokenStore().save_connection_tokens(db, connection, access_token="a", refresh_token="r")
        connection.connection_status = "revoked"
        db.commit()

        result = get_connector_ui_state(db, location_id, "tiktok")
    finally:
        db.close()

    assert result["ui_status"] == UI_DISCONNECTED


# ======================================================================
# get_all_connector_ui_states() -- the whole-dashboard call
# ======================================================================

def test_get_all_connector_ui_states_returns_all_six_platforms(client):
    location_id = _register_and_onboard(client, "allsix")
    from database import get_session
    db = get_session()
    try:
        results = get_all_connector_ui_states(db, location_id)
    finally:
        db.close()

    platforms = {r["platform"] for r in results}
    assert platforms == {"facebook", "instagram", "google_business", "x", "threads", "tiktok"}
    for r in results:
        assert set(r.keys()) == {"platform", "ui_status", "capabilities", "selectable_for_publish"}
        assert r["ui_status"] in UI_STATES
        assert isinstance(r["selectable_for_publish"], bool)
