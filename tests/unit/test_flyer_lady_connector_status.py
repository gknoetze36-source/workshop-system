"""flyer_lady/connectors/status.py -- normalizing each connector's
existing health() signal into exactly the four required statuses:
connected, reconnect_required, disconnected, pending_review.

No new database state is exercised here beyond what
test_flyer_lady_connectors.py already covers for health() itself --
these tests are specifically about the mapping, and about
get_connector_status()/get_all_connector_statuses() actually returning
the {platform, status, capabilities} shape the UI/service needs.
"""
from __future__ import annotations

import re
from unittest.mock import patch

import pytest

from flyer_lady.connectors import (
    CONNECTED,
    DISCONNECTED,
    PENDING_REVIEW,
    RECONNECT_REQUIRED,
    VALID_STATUSES,
    get_all_connector_statuses,
    get_connector_status,
)
from flyer_lady.connectors.status import normalize_status


# --------------------------------------------------------------------
# normalize_status() -- the pure mapping, every real raw value
# --------------------------------------------------------------------

def test_connected_maps_to_connected():
    assert normalize_status(deployment_configured=True, raw_connection_status="connected") == CONNECTED


def test_reconnect_required_maps_to_reconnect_required():
    """The exact value flyer_lady/publish_service.py's except block
    already writes on a 401/403 (retry_policy.py) -- confirmed this
    passes through unchanged, not remapped to something else."""
    assert normalize_status(deployment_configured=True, raw_connection_status="reconnect_required") == RECONNECT_REQUIRED


def test_revoked_maps_to_disconnected():
    """The value services/meta_data_deletion.py, offboarding_service.py
    and GoogleBusinessConnector.revoke() all actually write."""
    assert normalize_status(deployment_configured=True, raw_connection_status="revoked") == DISCONNECTED


def test_not_connected_maps_to_disconnected():
    """The value each connector's own health() reports when no
    connection row exists at all -- not a stored value, but a real one
    this module must still handle correctly."""
    assert normalize_status(deployment_configured=True, raw_connection_status="not_connected") == DISCONNECTED


def test_pending_review_passes_through_even_though_nothing_writes_it_yet():
    """Supported, not fabricated: nothing in this codebase currently
    sets connection_status="pending_review", but if it ever is, this
    must not silently reclassify it as something else."""
    assert normalize_status(deployment_configured=True, raw_connection_status="pending_review") == PENDING_REVIEW


def test_unconfigured_deployment_always_means_disconnected():
    """Even a connection row that says "connected" is meaningless
    without live credentials -- deployment_configured=False must win
    regardless of what the stale row claims."""
    assert normalize_status(deployment_configured=False, raw_connection_status="connected") == DISCONNECTED


def test_unrecognized_raw_value_conservatively_maps_to_disconnected():
    """The connection_status column has no enum/CHECK constraint --
    defensive: an unexpected string must not be silently treated as
    connected."""
    assert normalize_status(deployment_configured=True, raw_connection_status="some_future_value_nobody_wrote_yet") == DISCONNECTED


def test_none_raw_value_maps_to_disconnected():
    assert normalize_status(deployment_configured=True, raw_connection_status=None) == DISCONNECTED


def test_exactly_the_four_required_statuses_exist():
    assert VALID_STATUSES == {"connected", "reconnect_required", "disconnected", "pending_review"}


# --------------------------------------------------------------------
# get_connector_status() / get_all_connector_statuses() -- the actual
# {platform, status, capabilities} lookup, through real connectors
# --------------------------------------------------------------------

def _register_and_onboard(client, suffix):
    email = f"connectorstatus-{suffix}@test.example"

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
        "location_name": f"Connector Status {suffix}", "industry": "workshop", "csrf_token": token2,
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


def test_get_connector_status_returns_exactly_platform_status_capabilities(client, monkeypatch):
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "123456789012345")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "a" * 32)

    from database import get_session
    location_id = _register_and_onboard(client, "shape")

    db = get_session()
    try:
        result = get_connector_status(db, location_id, "facebook")
    finally:
        db.close()

    assert set(result.keys()) == {"platform", "status", "capabilities"}
    assert result["platform"] == "facebook"
    assert result["status"] in VALID_STATUSES


def test_no_connection_at_all_reports_disconnected(client, monkeypatch):
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "123456789012345")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "a" * 32)

    from database import get_session
    location_id = _register_and_onboard(client, "noconn")

    db = get_session()
    try:
        result = get_connector_status(db, location_id, "facebook")
    finally:
        db.close()

    assert result["status"] == DISCONNECTED


def test_unconfigured_deployment_reports_disconnected(client, monkeypatch):
    """No META_FLYER_LADY_* set at all -- must report disconnected, not
    crash and not report connected."""
    location_id = _register_and_onboard(client, "unconfigured")

    from database import get_session
    db = get_session()
    try:
        result = get_connector_status(db, location_id, "facebook")
    finally:
        db.close()

    assert result["status"] == DISCONNECTED


def test_a_real_connected_facebook_connection_reports_connected(client, monkeypatch):
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "123456789012345")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "a" * 32)
    monkeypatch.setenv("META_FLYER_LADY_CONFIG_ID", "999888777")
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")

    from database import get_session
    from models.integration_models import MetaSocialConnection
    from integrations.meta.auth.token_store import MetaTokenStore

    location_id = _register_and_onboard(client, "connected")

    db = get_session()
    try:
        connection = MetaSocialConnection(
            location_id=location_id, meta_user_id="meta-user-1", page_id="page-1",
            page_name="Test Page", connection_status="connected", encrypted_page_access_token="",
        )
        db.add(connection)
        db.flush()
        MetaTokenStore().save_social_token(db, connection, "fake-token")
        db.commit()

        result = get_connector_status(db, location_id, "facebook")
    finally:
        db.close()

    assert result["status"] == CONNECTED
    assert result["capabilities"] == {"platform": "facebook", "post_types": ["facebook_feed", "facebook_story"]}


def test_a_reconnect_required_connection_reports_reconnect_required(client, monkeypatch):
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "123456789012345")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "a" * 32)
    monkeypatch.setenv("META_FLYER_LADY_CONFIG_ID", "999888777")
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")

    from database import get_session
    from models.integration_models import MetaSocialConnection
    from integrations.meta.auth.token_store import MetaTokenStore

    location_id = _register_and_onboard(client, "reconnectreq")

    db = get_session()
    try:
        connection = MetaSocialConnection(
            location_id=location_id, meta_user_id="meta-user-1", page_id="page-1",
            page_name="Test Page", connection_status="connected", encrypted_page_access_token="",
        )
        db.add(connection)
        db.flush()
        # save_social_token() itself unconditionally sets
        # connection_status="connected" -- so the retry_policy.py-style
        # reconnect_required state this test is actually about must be
        # applied AFTER, exactly matching the real order of events (a
        # connection is saved as connected, and only later, on an
        # auth failure, gets marked reconnect_required).
        MetaTokenStore().save_social_token(db, connection, "fake-token")
        connection.connection_status = "reconnect_required"
        db.commit()

        result = get_connector_status(db, location_id, "facebook")
    finally:
        db.close()

    assert result["status"] == RECONNECT_REQUIRED


def test_get_all_connector_statuses_returns_all_six_registered_platforms(client, monkeypatch):
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "123456789012345")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "a" * 32)

    from database import get_session
    location_id = _register_and_onboard(client, "allplatforms")

    db = get_session()
    try:
        results = get_all_connector_statuses(db, location_id)
    finally:
        db.close()

    platforms = {r["platform"] for r in results}
    assert platforms == {"facebook", "instagram", "google_business", "x", "threads", "tiktok"}
    for r in results:
        assert set(r.keys()) == {"platform", "status", "capabilities"}
        assert r["status"] in VALID_STATUSES


def test_status_lookup_uses_the_real_health_and_capabilities_not_new_logic():
    """Proves get_connector_status() genuinely delegates to the
    existing, unchanged connector methods rather than querying
    anything itself."""
    from flyer_lady.connectors.meta_social import FacebookConnector

    with patch.object(
        FacebookConnector, "health",
        return_value={"platform": "facebook", "deployment_configured": True,
                      "deployment_missing": [], "connection_status": "connected"},
    ) as mock_health, patch.object(
        FacebookConnector, "capabilities",
        return_value={"platform": "facebook", "post_types": ["facebook_feed", "facebook_story"]},
    ) as mock_caps:
        result = get_connector_status(session="fake-session", location_id=1, platform="facebook")

    mock_health.assert_called_once_with("fake-session", 1)
    mock_caps.assert_called_once_with()
    assert result == {
        "platform": "facebook", "status": CONNECTED,
        "capabilities": {"platform": "facebook", "post_types": ["facebook_feed", "facebook_story"]},
    }
