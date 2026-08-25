"""Regression test for two bugs found 2026-08-25 auditing templates and
error handling.

1. routes/error.py registered its 404/500 handlers via
   @error_bp.errorhandler(...) on a Blueprint with no routes of its own.
   Flask blueprint-scoped error handlers only fire for errors raised by
   routes registered on that same blueprint -- with none registered here,
   these could never actually fire for a genuine unmatched URL or
   unhandled exception anywhere else in the app. Confirmed directly: a
   request to a nonexistent URL returned Werkzeug's bare default 404
   page, not templates/404.html. The custom error pages had never
   actually been served. Fixed by registering as app-level handlers.

2. templates/404.html and templates/500.html checked
   `current_user.is_authenticated`, but current_user() (services/
   auth_service.py) returns a plain session dict with no such key --
   Jinja's dict-key fallback for dot notation means this was always
   Undefined/falsy, so a logged-in user hitting either error page always
   saw the anonymous-visitor branch (a login link) instead of a link back
   to their dashboard. Fixed to check `current_user` directly, matching
   the correct pattern templates/base.html already used.
"""
import re


def test_404_uses_custom_template_not_werkzeug_default():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    response = client.get("/this-page-does-not-exist-xyz")
    assert response.status_code == 404
    html = response.get_data(as_text=True)
    assert "Go to Login" in html or "Return to Dashboard" in html, \
        "must render the custom 404.html, not Werkzeug's bare default page"


def test_404_shows_login_link_for_anonymous_visitor():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    response = client.get("/this-page-does-not-exist-xyz")
    html = response.get_data(as_text=True)
    assert "Go to Login" in html
    assert "Return to Dashboard" not in html


def test_404_shows_dashboard_link_for_logged_in_user():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html)
        return m.group(1) if m else None

    token = csrf_from("/register")
    client.post("/register", data={
        "full_name": "Test", "email": "errpagetest@test.example", "password": "SuperSecret123",
        "confirm_password": "SuperSecret123", "csrf_token": token,
    })
    token2 = csrf_from("/onboarding/location")
    client.post("/onboarding/location", data={
        "location_name": "Error Page Test Workshop", "industry": "workshop", "csrf_token": token2,
    })

    response = client.get("/this-page-does-not-exist-xyz")
    html = response.get_data(as_text=True)
    assert "Return to Dashboard" in html, "a logged-in user must see the dashboard link, not the login link"
    assert "Go to Login" not in html


def test_500_handler_is_registered_app_wide_not_blueprint_scoped():
    """Directly verifies the fix at the source: Flask's internal
    error_handler_spec nests handlers as spec[blueprint_name][status_code].
    The bug was registering under spec['error'][500] (blueprint-scoped,
    on a blueprint with no routes -- so it could never fire for errors
    anywhere else in the app); the fix registers under spec[None][500]
    (app-wide). A live-exception-triggering test is deliberately avoided
    here since Flask locks route registration after the first request,
    which every other test in this file (and the shared test session)
    has already triggered by the time this one would run."""
    import phanta_app
    app_wide_handlers = phanta_app.app.error_handler_spec.get(None, {})
    assert 404 in app_wide_handlers, "404 handler must be registered app-wide, not blueprint-scoped"
    assert 500 in app_wide_handlers, "500 handler must be registered app-wide, not blueprint-scoped"
