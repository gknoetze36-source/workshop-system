"""After onboarding the Ghost asks to be clicked, greets the workshop and points to
the Meta and Google setup guides; the guides render as PHANTA pages."""
import re
import subprocess
import uuid
from unittest.mock import patch

from tests.unit.test_dashboard_actions import _register_and_onboard


def _client():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    _, csrf_from = _register_and_onboard(client, "ghost" + uuid.uuid4().hex[:6])
    return client, csrf_from


def test_onboarding_complete_turns_on_the_ghost_welcome_until_clicked():
    client, csrf_from = _client()
    assert "PHANTA_GHOST_WELCOME" not in client.get("/dashboard").get_data(as_text=True)
    token = csrf_from("/dashboard")
    with patch("routes.onboarding.required_outstanding", return_value=[]), \
         patch("routes.onboarding.active_location_required", return_value=None):
        resp = client.post("/onboarding/complete", data={"csrf_token": token})
    assert resp.status_code == 302
    page = client.get("/dashboard").get_data(as_text=True)
    assert 'window.PHANTA_GHOST_WELCOME = {meta: "/help/meta", google: "/help/google"}' in page
    assert "PHANTA_GHOST_WELCOME" in client.get("/customers").get_data(as_text=True)  # every page until clicked

    assert client.post("/api/ghost/welcome-seen", headers={"X-CSRFToken": csrf_from("/dashboard")}).status_code == 200
    assert "PHANTA_GHOST_WELCOME" not in client.get("/dashboard").get_data(as_text=True)


def test_setup_guides_render_for_logged_in_users_only():
    client, _ = _client()
    meta = client.get("/help/meta").get_data(as_text=True)
    assert "<h1>Setting up Meta" in meta and "Add payment method" in meta and "<table>" in meta
    assert "<h1>Setting up Google Ads" in client.get("/help/google").get_data(as_text=True)
    assert client.get("/help/nope").status_code == 404
    import phanta_app
    assert phanta_app.app.test_client().get("/help/meta").status_code in (302, 401)


def test_ghost_script_is_valid_javascript(tmp_path):
    js = tmp_path / "ghost.js"
    js.write_text(open("static/js/phanta-ghost.js").read())
    result = subprocess.run(["node", "--check", str(js)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert re.search(r"Please click me", js.read_text())
