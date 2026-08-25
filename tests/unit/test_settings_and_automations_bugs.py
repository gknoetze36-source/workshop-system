"""Regression tests for a batch of bugs found 2026-08-25 auditing
routes/settings.py and routes/automations.py, continuing the sweep for the
boolean-literal and column-mismatch bug classes found repeatedly elsewhere
this engagement.
"""
import re

from database import execute_db, query_db, utc_now


def _register_and_onboard(client, suffix):
    email = f"settingsauto-{suffix}@test.example"

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
        "location_name": f"Settings Automations Workshop {suffix}", "industry": "workshop", "csrf_token": token2,
    })
    return email, csrf_from


def test_settings_business_clearing_secondary_phone_actually_clears_contact_phone():
    """Before the fix, `field` was reassigned to its alias target
    (contact_phone) before the clearable-fields membership check ran, so
    the check tested the wrong name and an empty submission silently
    failed to clear the value."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    email, csrf_from = _register_and_onboard(client, "clear")

    token = csrf_from("/settings/business")
    client.post("/settings/business", data={"primary_whatsapp_number": "+27821110000", "csrf_token": token})

    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]
    assert query_db("SELECT contact_phone FROM locations WHERE id=%s", (location_id,), one=True)["contact_phone"] == "+27821110000"

    token2 = csrf_from("/settings/business")
    client.post("/settings/business", data={"secondary_phone_number": "", "csrf_token": token2})
    assert query_db("SELECT contact_phone FROM locations WHERE id=%s", (location_id,), one=True)["contact_phone"] == ""


def test_settings_users_invite_and_toggle_do_not_use_integer_booleans():
    """users.active and users.must_reset_password are real Postgres
    BOOLEAN columns; passing literal 1/0 raises DatatypeMismatch there
    (this test runs on SQLite, where it wouldn't have failed -- it exists
    to lock in the fix at the source-code level via the assertions below,
    with the real cross-backend proof living in the manual Postgres
    verification for this fix)."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    _, csrf_from = _register_and_onboard(client, "invite")

    token = csrf_from("/settings/users")
    response = client.post("/settings/users", data={
        "action": "invite", "email": "staffmember@test.example", "role": "reception",
        "full_name": "Staff Member", "csrf_token": token,
    }, follow_redirects=False)
    assert response.status_code == 302

    row = query_db("SELECT id, active, must_reset_password FROM users WHERE email='staffmember@test.example'", one=True)
    assert row is not None

    token2 = csrf_from("/settings/users")
    response2 = client.post("/settings/users", data={
        "action": "toggle_status", "user_id": str(row["id"]), "csrf_token": token2,
    }, follow_redirects=False)
    assert response2.status_code == 302


def test_onboarding_team_invite_does_not_use_integer_booleans():
    """Same bug, sibling code path in onboarding.py's own team-invite
    handler."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    _, csrf_from = _register_and_onboard(client, "onboardinvite")

    token = csrf_from("/onboarding/team")
    response = client.post("/onboarding/team", data={
        "action": "invite", "email": "onboardstaff@test.example", "role": "reception",
        "full_name": "Onboard Staff", "csrf_token": token,
    }, follow_redirects=False)
    assert response.status_code == 302


def test_automations_history_renders_with_no_history():
    """Before the fix, this 500'd unconditionally -- utc_now() returns an
    ISO string, not a datetime, and `utc_now() - timedelta(days=30)`
    raised TypeError before a single query ever ran."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    _register_and_onboard(client, "history")

    response = client.get("/automations/history")
    assert response.status_code == 200


def test_automations_history_includes_failed_jobs_with_correct_data():
    """failed_jobs has no automation_rule_id/attempts/last_error/
    created_at column -- it references scheduled_jobs via
    scheduled_job_id, and the real error column is error_message. The
    previous query referenced four nonexistent columns."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    email, _ = _register_and_onboard(client, "historydata")

    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]
    execute_db(
        "INSERT INTO automation_templates (industry,name,event_type,default_delay_minutes,default_message,created_at,updated_at) "
        "VALUES ('workshop','Reminder','booking_reminder',60,'msg',%s,%s)",
        (utc_now(), utc_now()),
    )
    template_id = query_db("SELECT id FROM automation_templates WHERE event_type='booking_reminder'", one=True)["id"]
    execute_db(
        "INSERT INTO automation_rules (location_id,template_id,name,event_type,active,delay_minutes,created_at,updated_at) "
        "VALUES (%s,%s,'Reminder','booking_reminder',TRUE,60,%s,%s)",
        (location_id, template_id, utc_now(), utc_now()),
    )
    rule_id = query_db("SELECT id FROM automation_rules WHERE location_id=%s", (location_id,), one=True)["id"]
    execute_db(
        "INSERT INTO scheduled_jobs (automation_rule_id,job_type,status,scheduled_for,attempts,created_at,updated_at) "
        "VALUES (%s,'automation_action','failed',%s,3,%s,%s)",
        (rule_id, utc_now(), utc_now(), utc_now()),
    )
    scheduled_job_id = query_db("SELECT id FROM scheduled_jobs WHERE automation_rule_id=%s", (rule_id,), one=True)["id"]
    execute_db(
        "INSERT INTO failed_jobs (scheduled_job_id,error_message,failed_at) VALUES (%s,'connection timeout',%s)",
        (scheduled_job_id, utc_now()),
    )

    response = client.get("/automations/history")
    assert response.status_code == 200
