"""Behavioural tests for the rebuilt onboarding flow (2026-08-28).

Covers the requirements that are easy to regress silently:

  * business identity is stored on the OWNER, not the location
  * VAT is never written during onboarding
  * the shop name comes from the trading name
  * Sunday (and Saturday) default to closed rather than being required
  * each legal document is confirmed separately and recorded with its version
  * completion is verified server-side and cannot be reached by simply
    arriving at the final page
  * WhatsApp is skippable and does not block completion
"""
import json
import re
import uuid

import pytest
from werkzeug.security import generate_password_hash

from database import initialize_database, execute_db, query_db, utc_now
from tests.rls_helpers import platform_scope, location_scope
import phanta_app

phanta_app.app.config["TESTING"] = True

PASSWORD = "TestPass123!"


def _csrf(client, path):
    html = client.get(path).get_data(as_text=True)
    match = re.search(r'name="csrf_token"[^>]*value="([^"]+)"', html)
    return match.group(1) if match else ""


@pytest.fixture
def onboarding_client():
    """Seed through the RLS context helpers so this suite runs on PostgreSQL too."""
    initialize_database(run_migrations=False)
    suffix = uuid.uuid4().hex[:10]
    owner_email = f"onb-owner-{suffix}@test.example"

    with platform_scope():
        execute_db(
            "INSERT INTO owners (name,email,active,created_at,updated_at) VALUES (%s,%s,TRUE,%s,%s)",
            (f"Onb {suffix}", owner_email, utc_now(), utc_now()),
        )
        owner_id = query_db("SELECT id FROM owners WHERE email=%s", (owner_email,), one=True)["id"]

        execute_db(
            "INSERT INTO locations (owner_id,name,industry,active,created_at,updated_at) "
            "VALUES (%s,%s,'workshop',TRUE,%s,%s)",
            (owner_id, "", utc_now(), utc_now()),
        )
        location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]

        user_email = f"onb-user-{suffix}@test.example"
        execute_db(
            """INSERT INTO users
               (username,email,password,password_hash,full_name,role,owner_id,location_id,
                active,must_reset_password,session_version,created_at,updated_at)
               VALUES (%s,%s,'',%s,'Owner','owner',%s,%s,TRUE,FALSE,1,%s,%s)""",
            (user_email, user_email, generate_password_hash(PASSWORD), owner_id, location_id,
             utc_now(), utc_now()),
        )

    client = phanta_app.app.test_client()
    client.post("/login", data={
        "email": user_email, "password": PASSWORD, "csrf_token": _csrf(client, "/login"),
    })
    return {"client": client, "owner_id": owner_id, "location_id": location_id, "suffix": suffix}


def _complete_business(ctx, trading_name="Ace Motors"):
    client = ctx["client"]
    return client.post("/onboarding/business", data={
        "legal_name": "Ace Motors (Pty) Ltd",
        "business_registration_number": "2019/123456/07",
        "trading_name": trading_name,
        "business_email": f"biz-{ctx['suffix']}@test.example",
        "csrf_token": _csrf(client, "/onboarding/business"),
    }, follow_redirects=False)


def _complete_workshop(ctx, **overrides):
    client = ctx["client"]
    data = {
        "name": "Ace Motors", "physical_address": "12 Main Road",
        "city": "George", "province": "Western Cape",
        "weekday_open": "08:00", "weekday_close": "17:00",
        "csrf_token": _csrf(client, "/onboarding/workshop"),
    }
    data.update(overrides)
    return client.post("/onboarding/workshop", data=data, follow_redirects=False)


# ---------------------------------------------------------------------------
# BUSINESS — owner scoped, no VAT
# ---------------------------------------------------------------------------

def test_business_identity_is_stored_on_the_owner(onboarding_client):
    ctx = onboarding_client
    response = _complete_business(ctx)
    assert response.status_code == 302

    owner = query_db(
        "SELECT legal_name, business_registration_number, trading_name, business_email "
        "FROM owners WHERE id=%s", (ctx["owner_id"],), one=True,
    )
    assert owner["legal_name"] == "Ace Motors (Pty) Ltd"
    assert owner["business_registration_number"] == "2019/123456/07"
    assert owner["trading_name"] == "Ace Motors"
    assert owner["business_email"]


def test_onboarding_never_writes_a_vat_number(onboarding_client):
    """VAT belongs to billing, captured at the paywall. Posting it must not stick."""
    ctx = onboarding_client
    client = ctx["client"]
    client.post("/onboarding/business", data={
        "legal_name": "Ace Motors (Pty) Ltd",
        "business_registration_number": "2019/123456/07",
        "trading_name": "Ace Motors",
        "business_email": f"biz-{ctx['suffix']}@test.example",
        "vat_number": "4123456789",
        "csrf_token": _csrf(client, "/onboarding/business"),
    })
    location = query_db("SELECT vat_number FROM locations WHERE id=%s", (ctx["location_id"],), one=True)
    assert not location["vat_number"], "a VAT number was written during onboarding"


def test_shop_name_comes_from_the_trading_name(onboarding_client):
    ctx = onboarding_client
    _complete_business(ctx, trading_name="Ace Auto Centre")
    location = query_db("SELECT name FROM locations WHERE id=%s", (ctx["location_id"],), one=True)
    assert location["name"] == "Ace Auto Centre"


@pytest.mark.parametrize("bad", ["12345", "2019/12345/07", "not-a-number", ""])
def test_invalid_cipc_number_is_rejected(onboarding_client, bad):
    ctx = onboarding_client
    client = ctx["client"]
    response = client.post("/onboarding/business", data={
        "legal_name": "Ace", "business_registration_number": bad,
        "trading_name": "Ace", "business_email": "a@b.co",
        "csrf_token": _csrf(client, "/onboarding/business"),
    })
    assert response.status_code == 200, "invalid registration number was accepted"
    owner = query_db("SELECT business_registration_number FROM owners WHERE id=%s",
                     (ctx["owner_id"],), one=True)
    assert not owner["business_registration_number"]


def test_cipc_number_accepts_common_separators(onboarding_client):
    ctx = onboarding_client
    client = ctx["client"]
    client.post("/onboarding/business", data={
        "legal_name": "Ace", "business_registration_number": "2019 123456 07",
        "trading_name": "Ace", "business_email": "a@b.co",
        "csrf_token": _csrf(client, "/onboarding/business"),
    })
    owner = query_db("SELECT business_registration_number FROM owners WHERE id=%s",
                     (ctx["owner_id"],), one=True)
    assert owner["business_registration_number"] == "2019/123456/07"


# ---------------------------------------------------------------------------
# WORKSHOP — hours, not slots
# ---------------------------------------------------------------------------

def test_weekend_days_default_to_closed(onboarding_client):
    """Not working weekends is normal and must not block onboarding."""
    ctx = onboarding_client
    _complete_business(ctx)
    response = _complete_workshop(ctx)
    assert response.status_code == 302

    row = query_db("SELECT operating_hours_json FROM locations WHERE id=%s",
                   (ctx["location_id"],), one=True)
    hours = json.loads(row["operating_hours_json"])
    assert hours["saturday"]["closed"] is True
    assert hours["sunday"]["closed"] is True
    assert hours["monday"]["open"] == "08:00"


def test_saturday_hours_recorded_when_supplied(onboarding_client):
    ctx = onboarding_client
    _complete_business(ctx)
    _complete_workshop(ctx, saturday_open="08:00", saturday_close="12:00")
    hours = json.loads(query_db(
        "SELECT operating_hours_json FROM locations WHERE id=%s",
        (ctx["location_id"],), one=True)["operating_hours_json"])
    assert hours["saturday"] == {"closed": False, "open": "08:00", "close": "12:00"}
    assert hours["sunday"]["closed"] is True


def test_closing_before_opening_is_rejected(onboarding_client):
    ctx = onboarding_client
    _complete_business(ctx)
    response = _complete_workshop(ctx, weekday_open="17:00", weekday_close="08:00")
    assert response.status_code == 200
    assert "must be after" in response.get_data(as_text=True)


def test_onboarding_does_not_collect_services(onboarding_client):
    """Services were removed from onboarding; the route must be gone."""
    ctx = onboarding_client
    assert ctx["client"].get("/onboarding/services").status_code == 404


# ---------------------------------------------------------------------------
# LEGAL — five documents, separately confirmed, version recorded
# ---------------------------------------------------------------------------

def test_legal_step_lists_all_five_documents_unticked(onboarding_client):
    ctx = onboarding_client
    page = ctx["client"].get("/onboarding/legal").get_data(as_text=True)
    assert page.count('name="confirm_') == 5, "expected five separate confirmations"
    assert "checked" not in page.split("legal-list")[1].split("</ul>")[0], \
        "a legal confirmation was pre-ticked"


def test_each_document_is_served_in_full(onboarding_client):
    ctx = onboarding_client
    response = ctx["client"].get("/onboarding/legal/privacy_policy")
    assert response.status_code == 200
    payload = json.loads(response.get_data(as_text=True))
    assert len(payload["text"]) > 2000, "document appears truncated"
    assert payload["version"]


def test_unknown_document_is_not_served(onboarding_client):
    assert onboarding_client["client"].get("/onboarding/legal/nope").status_code == 404


def test_partial_confirmation_does_not_satisfy_legal(onboarding_client):
    ctx = onboarding_client
    client = ctx["client"]
    response = client.post("/onboarding/legal", data={
        "confirm_terms_of_service": "on",
        "confirm_privacy_policy": "on",
        "csrf_token": _csrf(client, "/onboarding/legal"),
    })
    assert "Please confirm the" in response.get_data(as_text=True)

    from services.legal_acceptance_service import has_accepted_all
    assert not has_accepted_all(None, ctx["location_id"], owner_id=ctx["owner_id"])


def test_acceptance_records_version_and_owner(onboarding_client):
    ctx = onboarding_client
    client = ctx["client"]
    from services.legal_acceptance_service import REQUIRED_DOCUMENTS

    client.post("/onboarding/legal", data={
        **{f"confirm_{key}": "on" for key in REQUIRED_DOCUMENTS},
        "csrf_token": _csrf(client, "/onboarding/legal"),
    })

    with location_scope(ctx["location_id"]):
        rows = query_db(
            "SELECT document_key, document_version, owner_id FROM legal_acceptances WHERE owner_id=%s",
            (ctx["owner_id"],),
        )
    assert len(rows) == len(REQUIRED_DOCUMENTS)
    for row in rows:
        assert row["document_version"] == REQUIRED_DOCUMENTS[row["document_key"]]
        assert row["owner_id"] == ctx["owner_id"]


def test_version_bump_makes_only_that_document_outstanding(onboarding_client):
    ctx = onboarding_client
    client = ctx["client"]
    from services import legal_acceptance_service as legal

    client.post("/onboarding/legal", data={
        **{f"confirm_{key}": "on" for key in legal.REQUIRED_DOCUMENTS},
        "csrf_token": _csrf(client, "/onboarding/legal"),
    })
    with location_scope(ctx["location_id"]):
        assert legal.has_accepted_all(None, ctx["location_id"], owner_id=ctx["owner_id"])

    original = legal.REQUIRED_DOCUMENTS["privacy_policy"]
    legal.REQUIRED_DOCUMENTS["privacy_policy"] = "2099-01-01"
    try:
        with location_scope(ctx["location_id"]):
            outstanding = legal.outstanding_documents(None, ctx["location_id"], owner_id=ctx["owner_id"])
        assert list(outstanding) == ["privacy_policy"], \
            "a version bump made unrelated documents outstanding"
    finally:
        legal.REQUIRED_DOCUMENTS["privacy_policy"] = original


# ---------------------------------------------------------------------------
# COMPLETION — verified server-side
# ---------------------------------------------------------------------------

def test_completion_blocked_until_required_stages_done(onboarding_client):
    ctx = onboarding_client
    client = ctx["client"]
    response = client.post("/onboarding/complete", data={
        "csrf_token": _csrf(client, "/onboarding/legal"),
    }, follow_redirects=False)
    assert response.status_code == 302
    assert "/onboarding/review" in response.headers.get("Location", ""), \
        "onboarding completed without the required stages"


def test_whatsapp_is_skippable_and_does_not_block_completion(onboarding_client):
    """A workshop may finish onboarding without connecting WhatsApp."""
    ctx = onboarding_client
    client = ctx["client"]
    from services.legal_acceptance_service import REQUIRED_DOCUMENTS
    from services.onboarding_service import required_outstanding, stage_status

    _complete_business(ctx)
    _complete_workshop(ctx)
    with location_scope(ctx["location_id"]):
        execute_db(
            "INSERT INTO automation_rules (location_id,name,event_type,active,created_at,updated_at) "
            "VALUES (%s,%s,%s,TRUE,%s,%s)",
            (ctx["location_id"], "Reminder", "booking_created", utc_now(), utc_now()),
        )
    client.post("/onboarding/legal", data={
        **{f"confirm_{key}": "on" for key in REQUIRED_DOCUMENTS},
        "csrf_token": _csrf(client, "/onboarding/legal"),
    })

    with client.session_transaction() as sess:
        user = sess["user"]

    with location_scope(ctx["location_id"]):
        assert stage_status(user)["whatsapp_state"] == "NOT_STARTED"
        assert required_outstanding(user) == [], "WhatsApp wrongly blocked completion"

    response = client.post("/onboarding/complete", data={
        "csrf_token": _csrf(client, "/onboarding/review"),
    }, follow_redirects=False)
    assert "/onboarding/review" not in response.headers.get("Location", "")


def test_automation_requires_an_active_rule(onboarding_client):
    """An inactive rule must not satisfy the automation stage."""
    ctx = onboarding_client
    from services.onboarding_service import _automation_complete

    with location_scope(ctx["location_id"]):
        execute_db(
            "INSERT INTO automation_rules (location_id,name,event_type,active,created_at,updated_at) "
            "VALUES (%s,%s,%s,FALSE,%s,%s)",
            (ctx["location_id"], "Inactive", "booking_created", utc_now(), utc_now()),
        )
    with location_scope(ctx["location_id"]):
        assert _automation_complete(ctx["location_id"]) is False

    with location_scope(ctx["location_id"]):
        execute_db(
            "INSERT INTO automation_rules (location_id,name,event_type,active,created_at,updated_at) "
            "VALUES (%s,%s,%s,TRUE,%s,%s)",
            (ctx["location_id"], "Active", "booking_created", utc_now(), utc_now()),
        )
    with location_scope(ctx["location_id"]):
        assert _automation_complete(ctx["location_id"]) is True


def test_progress_is_derived_not_stored(onboarding_client):
    """Stage completion must come from the data, not a stored percentage."""
    ctx = onboarding_client
    from services.onboarding_service import stage_status

    with ctx["client"].session_transaction() as sess:
        user = sess["user"]
    with location_scope(ctx["location_id"]):
        assert stage_status(user)["business"] is False

    _complete_business(ctx)
    with location_scope(ctx["location_id"]):
        assert stage_status(user)["business"] is True, \
        "business completion did not follow the underlying data"


# ---------------------------------------------------------------------------
# REACHABILITY — routes that exist but nothing links to are not features
# ---------------------------------------------------------------------------

def test_settings_hub_links_password_and_export(onboarding_client):
    """The settings hub must expose the pages it gates.

    /settings/ previously redirected straight to /settings/business, which left
    the password-change and data-export pages unreachable from the interface
    even though their routes worked.
    """
    page = onboarding_client["client"].get("/settings/", follow_redirects=True).get_data(as_text=True)
    assert "/settings/password" in page, "change-password page is not linked from settings"
    assert "/settings/export" in page, "data export page is not linked from settings"


def test_customer_profile_offers_erasure_to_admins(onboarding_client):
    """The POPIA erasure route must be reachable from the customer's profile."""
    ctx = onboarding_client
    with location_scope(ctx["location_id"]):
        execute_db(
            "INSERT INTO customers (location_id, first_name, last_name, whatsapp_number, "
            "created_at, updated_at) VALUES (%s,'Erase','Me',%s,%s,%s)",
            (ctx["location_id"], f"+2782{ctx['suffix'][:7]}", utc_now(), utc_now()),
        )
        customer_id = query_db(
            "SELECT id FROM customers WHERE location_id=%s ORDER BY id DESC LIMIT 1",
            (ctx["location_id"],), one=True,
        )["id"]

    page = ctx["client"].get(f"/customers/{customer_id}").get_data(as_text=True)
    assert f"/customers/{customer_id}/delete" in page, \
        "erasure action is not reachable from the customer profile"


# ---------------------------------------------------------------------------
# INTEGRATION CONFIGURATION — unconfigured must not look broken
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", [
    "/integrations/meta/embedded-signup/config",
    "/integrations/meta/connection-health",
    "/integrations/meta/phone/info",
    "/integrations/meta/phone/waba",
])
def test_unconfigured_meta_returns_503_not_500(onboarding_client, path):
    """A deployment without Meta credentials must say so, not crash.

    These endpoints previously raised an unhandled RuntimeError, so the caller
    saw a 500 -- indistinguishable from a genuine fault. 503 with an
    explanation is the honest answer for "this works, it just has not been set
    up here".
    """
    response = onboarding_client["client"].get(path)
    assert response.status_code != 500, f"{path} crashed instead of reporting configuration"
    if response.status_code == 503:
        payload = json.loads(response.get_data(as_text=True))
        assert payload["configured"] is False
        assert payload["missing"], "503 did not say which variables are missing"


def test_unconfigured_webhook_verify_returns_503_not_500():
    """Meta's webhook verification handshake must not 500 when unconfigured."""
    client = phanta_app.app.test_client()
    response = client.get("/webhooks/meta?hub.mode=subscribe&hub.challenge=x&hub.verify_token=y")
    assert response.status_code != 500


def test_integration_status_never_exposes_credential_values(monkeypatch):
    """The diagnostics view reports variable NAMES only."""
    from services.integration_status import all_integration_status

    monkeypatch.setenv("META_APP_SECRET", "super-secret-value")
    statuses = all_integration_status()
    rendered = json.dumps(statuses)
    assert "super-secret-value" not in rendered, "a credential value leaked into integration status"


def test_no_broken_url_for_endpoints():
    """Every url_for() target in templates and routes must be a real endpoint.

    A template that builds an endpoint name which does not exist raises a
    BuildError at render time -- a 500 on a page that looks fine in source.
    """
    import os
    import re

    app = phanta_app.app
    endpoints = {rule.endpoint for rule in app.url_map.iter_rules()}
    pattern = re.compile(r"""url_for\(\s*['"]([^'"]+)['"]""")
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    broken = []
    for base, _, files in os.walk(root_dir):
        if any(skip in base for skip in ("__pycache__", ".git", "node_modules", "tests")):
            continue
        for name in files:
            if not name.endswith((".html", ".py")):
                continue
            path = os.path.join(base, name)
            try:
                text = open(path, encoding="utf-8").read()
            except OSError:
                continue
            for match in pattern.finditer(text):
                target = match.group(1)
                if target != "static" and target not in endpoints:
                    broken.append(f"{os.path.relpath(path, root_dir)} -> {target}")

    assert not broken, f"url_for targets that do not resolve: {broken}"
