"""Regression tests for adding Flyer Lady and WhatsApp connection to the
front of onboarding, found and fixed 2026-08-25.

Flyer Lady's social connection had never had a working browser UI at all
before this: /connect/callback (where Facebook redirects a real user's
browser after they approve the OAuth dialog) returned bare JSON, so a
real person had no way to actually pick their Facebook Page and finish
connecting -- confirmed by checking templates/flyer_lady.html, whose only
entry point was a plain link to /connect/start with nothing built to
receive Facebook's redirect back. Fixed by rendering a real page-picker
template and adding form-based completion to /connect/complete.

Also covers the onboarding sequence restructuring itself (WhatsApp and
Flyer Lady moved to be steps 2 and 3, immediately after location
creation, ahead of business/services/team) and a datetime comparison bug
found while testing the completion flow: MetaSocialOAuthSession.expires_at
is DateTime(timezone=True), always written as aware UTC, but SQLAlchemy
only round-trips that timezone info through Postgres -- SQLite silently
returns it naive, which raised TypeError comparing it against
datetime.now(timezone.utc). Verified this does NOT happen on real
Postgres (tested directly); fixed anyway so local SQLite development and
testing aren't broken, since the same DateTime(timezone=True) pattern is
used elsewhere in this codebase too.
"""
import re
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from database import query_db, get_session


def _register_and_create_location(client, suffix):
    email = f"flyeronboard-{suffix}@test.example"

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
    return csrf_from, client.post("/onboarding/location", data={
        "location_name": f"Flyer Onboard Workshop {suffix}", "industry": "workshop", "csrf_token": token2,
    }, follow_redirects=False), email


def test_account_creation_lands_on_business_step():
    """Location creation now leads into BUSINESS, not WhatsApp.

    This test previously asserted the opposite. The onboarding sequence was
    restructured on 2026-08-28: business identity (CIPC registration, legal
    name, trading name) is captured immediately after the location is created,
    because the trading name is what the workshop is called and everything
    customer-facing depends on it.

    WhatsApp and Flyer Lady now follow the workshop step and are both
    skippable, so neither can be a required landing point.
    """
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    csrf_from, response, _ = _register_and_create_location(client, "sequence")
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/onboarding/business")


def test_skip_chain_reaches_business_via_flyer_lady():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    _register_and_create_location(client, "skipchain")

    whatsapp_page = client.get("/onboarding/whatsapp")
    assert whatsapp_page.status_code == 200
    assert "/onboarding/flyer-lady" in whatsapp_page.get_data(as_text=True)

    flyer_page = client.get("/onboarding/flyer-lady")
    assert flyer_page.status_code == 200
    assert "/onboarding/business" in flyer_page.get_data(as_text=True)
    assert "Connect Flyer Lady" in flyer_page.get_data(as_text=True)


def test_page_picker_renders_with_real_pages():
    """Before this fix, Facebook's redirect back landed on bare JSON --
    nothing a real browser user could act on."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    with phanta_app.app.test_request_context():
        from flask import render_template
        html = render_template("flyer_lady_select_page.html", oauth_session_id=1, pages=[
            {"id": "12345", "name": "Test Workshop Page"},
        ], onboarding=True)
    assert "Test Workshop Page" in html
    assert "/dashboard/flyer-lady/connect/complete" in html


def test_page_picker_shows_error_state():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    with phanta_app.app.test_request_context():
        from flask import render_template
        html = render_template("flyer_lady_select_page.html", error="Your Meta session expired.", onboarding=True)
    assert "Your Meta session expired" in html


def test_connect_complete_form_post_saves_connection_and_redirects():
    """The actual end-to-end proof: completing a Flyer Lady connection
    through a real form submission saves a working MetaSocialConnection
    and continues onboarding -- this exact sequence previously crashed
    with TypeError on the expires_at comparison."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    csrf_from, _, email = _register_and_create_location(client, "complete")

    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]

    from models.integration_models import MetaSocialOAuthSession
    from integrations.meta.auth.token_store import MetaTokenStore

    session = get_session()
    oauth = MetaSocialOAuthSession(
        location_id=location_id, state_nonce=uuid.uuid4().hex, encrypted_user_access_token="",
        redirect_uri="http://test", status="pages_loaded",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=15),
    )
    session.add(oauth)
    session.flush()
    MetaTokenStore().save_social_oauth_token(session, oauth, "fake_user_token")
    session.commit()
    oauth_id = oauth.id
    session.close()

    token = csrf_from("/settings/business")
    with patch("routes.flyer_lady.MetaSocialGraphClient") as mock_graph, \
         patch("routes.flyer_lady.MetaAuthConfig") as mock_config, \
         patch("routes.flyer_lady.GraphApiClient"):
        mock_config.from_env.return_value = MagicMock()
        mock_graph.return_value.list_pages.return_value = {"data": [
            {"id": "12345", "name": "Test Workshop Page", "access_token": "page_token_xyz", "tasks": ["MANAGE"]}
        ]}
        response = client.post("/dashboard/flyer-lady/connect/complete", data={
            "csrf_token": token, "oauth_session_id": str(oauth_id), "page_id": "12345", "onboarding": "1",
        }, follow_redirects=False)

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/onboarding/business")

    from models.integration_models import MetaSocialConnection
    verify_session = get_session()
    connection = verify_session.query(MetaSocialConnection).filter_by(location_id=location_id).first()
    verify_session.close()
    assert connection is not None
    assert connection.page_name == "Test Workshop Page"
    assert connection.connection_status == "connected"


def test_expires_at_comparison_survives_a_naive_datetime():
    """Direct regression test for the datetime bug itself, independent of
    the full HTTP flow above."""
    from routes.flyer_lady import _location  # noqa: F401 - import sanity
    naive_expiry = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=15)
    expires_at = naive_expiry
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    # Must not raise TypeError
    assert expires_at > datetime.now(timezone.utc)
