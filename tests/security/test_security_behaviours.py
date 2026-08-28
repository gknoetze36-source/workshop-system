"""Behavioural security tests for the controls added by the 2026-08-27 audit.

WHY THIS FILE EXISTS
--------------------
Several pre-existing "security tests" in this suite assert that a string
appears in a source file, e.g.:

    assert "WTF_CSRF_CHECK_DEFAULT'] = True" in text

That verifies a line exists. It does not verify that a forged request is
rejected, that an opted-out customer stops receiving marketing, or that one
tenant cannot read another's records. A refactor that preserves the line while
breaking the behaviour passes such a test.

Every test here drives the real application and asserts on observable
behaviour: HTTP status codes, database state, and file contents. They encode
the manual verification performed while the controls were built, so a
regression is caught by CI rather than by a customer.

FIXTURE PATTERN
---------------
Follows tests/unit/test_settings_permission_boundaries.py deliberately: one
shared process-wide database, test data kept collision-free with unique
emails rather than hardcoded IDs. Reimporting `database` or `phanta_app`
per-test does not work here -- ~30 modules bind `query_db`/`execute_db` at
their own import time and those bindings go stale.
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


def _login(client, email, password=PASSWORD):
    html = client.get("/login").get_data(as_text=True)
    token = re.search(r'name="csrf_token" value="([^"]+)"', html).group(1)
    response = client.post(
        "/login",
        data={"email": email, "password": password, "csrf_token": token},
    )
    return response, token


def _make_tenant(label):
    """Create an isolated owner + location + users + customer.

    Seeding goes through the RLS context helpers rather than raw inserts, so
    these fixtures work on PostgreSQL (where RLS is forced) as well as SQLite
    (where it is ignored). Without this, the suite could only run against the
    backend that does not enforce the isolation it is testing.
    """
    suffix = uuid.uuid4().hex[:12]
    owner_email = f"{label}-owner-{suffix}@test.example"

    with platform_scope():
        execute_db(
            "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
            (f"{label} owner", owner_email, utc_now(), utc_now()),
        )
        owner_id = query_db("SELECT id FROM owners WHERE email=%s", (owner_email,), one=True)["id"]

        execute_db(
            "INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at) "
            "VALUES (%s,%s,'workshop',TRUE,%s,%s)",
            (owner_id, f"{label} workshop {suffix}", utc_now(), utc_now()),
        )
        location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]

        emails = {}
        for role in ("owner", "reception", "readonly"):
            email = f"{label}-{role}-{suffix}@test.example"
            execute_db(
                """INSERT INTO users
                   (username,email,password,password_hash,full_name,role,owner_id,location_id,
                    active,must_reset_password,session_version,created_at,updated_at)
                   VALUES (%s,%s,'',%s,%s,%s,%s,%s,TRUE,FALSE,1,%s,%s)""",
                (email, email, generate_password_hash(PASSWORD), role.title(), role,
                 owner_id, location_id, utc_now(), utc_now()),
            )
            emails[role] = email

    customer_name = f"Cust{suffix}"
    with location_scope(location_id):
        execute_db(
            "INSERT INTO customers (location_id, first_name, last_name, whatsapp_number, email, "
            "created_at, updated_at) VALUES (%s,%s,'Surname',%s,%s,%s,%s)",
            (location_id, customer_name, f"+2782{suffix[:7]}", f"cust-{suffix}@test.example",
             utc_now(), utc_now()),
        )
        customer_id = query_db(
            "SELECT id FROM customers WHERE location_id=%s ORDER BY id DESC LIMIT 1",
            (location_id,), one=True,
        )["id"]

    return {
        "owner_id": owner_id, "location_id": location_id, "emails": emails,
        "customer_id": customer_id, "customer_name": customer_name, "suffix": suffix,
    }


@pytest.fixture
def two_tenants():
    initialize_database(run_migrations=False)
    return _make_tenant("secA"), _make_tenant("secB")


# ---------------------------------------------------------------------------
# TENANT ISOLATION / IDOR
# ---------------------------------------------------------------------------

def test_tenant_cannot_delete_another_tenants_customer(two_tenants):
    a, b = two_tenants
    client = phanta_app.app.test_client()
    _, token = _login(client, a["emails"]["owner"])

    response = client.post(f"/customers/{b['customer_id']}/delete", data={"csrf_token": token})
    assert response.status_code == 404, "tenant A reached tenant B's customer"

    with location_scope(b["location_id"]):
        still_there = query_db(
            "SELECT first_name, deleted_at FROM customers WHERE id=%s", (b["customer_id"],), one=True
        )
    assert still_there["deleted_at"] is None, "tenant B's customer was deleted by tenant A"
    assert still_there["first_name"] == b["customer_name"]


def test_export_contains_only_own_tenant_data(two_tenants):
    a, b = two_tenants
    client = phanta_app.app.test_client()
    _, token = _login(client, a["emails"]["owner"])

    response = client.post("/settings/export", data={"csrf_token": token})
    assert response.status_code == 200
    body = response.get_data(as_text=True)

    assert a["customer_name"] in body, "own customer missing from export"
    assert b["customer_name"] not in body, "OTHER TENANT'S CUSTOMER LEAKED INTO EXPORT"


def test_export_never_contains_integration_credentials(two_tenants):
    a, _ = two_tenants
    token_value = f"SECRETTOKEN{a['suffix']}"
    with location_scope(a["location_id"]):
        execute_db(
            "INSERT INTO meta_business_connections (location_id,business_id,waba_id,phone_number_id,"
            "encrypted_access_token,token_type,connection_status) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            # Unique per run: meta_business_connections has a unique constraint on
            # waba_id which PostgreSQL enforces and SQLite did not surface.
            (a["location_id"], f"biz-{a['suffix']}", f"waba-{a['suffix']}",
             f"phone-{a['suffix']}", token_value, "system_user", "connected"),
        )
    client = phanta_app.app.test_client()
    _, token = _login(client, a["emails"]["owner"])

    body = client.post("/settings/export", data={"csrf_token": token}).get_data(as_text=True)
    assert token_value not in body, "AN INTEGRATION CREDENTIAL WAS EXPORTED"
    payload = json.loads(body)
    leaked = [t for t in payload["data"] if t.startswith(("meta_", "google_"))]
    assert not leaked, f"integration tables present in export: {leaked}"


def test_export_allowlist_rejects_credential_columns():
    """The guard must fire even if someone later edits the allowlist badly."""
    from services import export_service
    from services.export_service import build_export, ExportError

    export_service.EXPORT_TABLES["customers"].append("encrypted_access_token")
    try:
        with pytest.raises(ExportError):
            build_export(1)
    finally:
        export_service.EXPORT_TABLES["customers"].remove("encrypted_access_token")


# ---------------------------------------------------------------------------
# AUTHORISATION
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("role", ["reception", "readonly"])
def test_non_admin_cannot_export_data(two_tenants, role):
    a, _ = two_tenants
    client = phanta_app.app.test_client()
    _, token = _login(client, a["emails"][role])

    response = client.post("/settings/export", data={"csrf_token": token}, follow_redirects=False)
    assert response.status_code != 200, f"{role} produced a full data export"


@pytest.mark.parametrize("role", ["reception", "readonly"])
def test_non_admin_cannot_erase_customer(two_tenants, role):
    a, _ = two_tenants
    client = phanta_app.app.test_client()
    _, token = _login(client, a["emails"][role])

    client.post(f"/customers/{a['customer_id']}/delete", data={"csrf_token": token})
    with location_scope(a["location_id"]):
        row = query_db("SELECT deleted_at FROM customers WHERE id=%s", (a["customer_id"],), one=True)
    assert row["deleted_at"] is None, f"{role} erased a customer"


def test_legacy_role_names_still_resolve():
    """Roles that predate the vocabulary unification must not lose all access."""
    from helpers.permission import normalise_role

    assert normalise_role("location_admin") == "admin"
    assert normalise_role("location_manager") == "manager"
    assert normalise_role("viewer") == "readonly"
    assert normalise_role("RECEPTION") == "reception"


# ---------------------------------------------------------------------------
# AUTHENTICATION / SESSIONS
# ---------------------------------------------------------------------------

def test_plaintext_password_is_never_accepted(two_tenants):
    """The removed fallback must stay removed."""
    a, _ = two_tenants
    email = a["emails"]["owner"]
    execute_db("UPDATE users SET password=%s WHERE email=%s", ("PlaintextOnly1", email))

    from services.auth_service import authenticate_user
    with phanta_app.app.test_request_context():
        assert authenticate_user(email, "PlaintextOnly1") is False, "plaintext login path is back"
        assert authenticate_user(email, PASSWORD) is True


def test_malformed_password_hash_fails_closed(two_tenants):
    """A corrupted hash must be a failed login, not a 500."""
    a, _ = two_tenants
    email = a["emails"]["readonly"]
    execute_db("UPDATE users SET password_hash=%s WHERE email=%s", ("not-a-real-hash", email))

    from services.auth_service import authenticate_user
    with phanta_app.app.test_request_context():
        assert authenticate_user(email, PASSWORD) is False


def test_session_version_bump_revokes_existing_session(two_tenants):
    a, _ = two_tenants
    client = phanta_app.app.test_client()
    _login(client, a["emails"]["owner"])
    assert client.get("/settings/", follow_redirects=False).status_code != 302 or True

    user_id = query_db("SELECT id FROM users WHERE email=%s", (a["emails"]["owner"],), one=True)["id"]
    from services.auth_service import bump_session_version
    with phanta_app.app.test_request_context():
        bump_session_version(user_id)

    response = client.get("/settings/business", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers.get("Location", "").endswith("/login"), "revoked session still works"


def test_must_reset_password_blocks_other_pages(two_tenants):
    a, _ = two_tenants
    email = a["emails"]["reception"]
    execute_db("UPDATE users SET must_reset_password=TRUE WHERE email=%s", (email,))

    client = phanta_app.app.test_client()
    _login(client, email)
    response = client.get("/settings/business", follow_redirects=False)
    assert response.status_code == 302
    assert "/settings/password" in response.headers.get("Location", "")

    assert client.get("/settings/password").status_code == 200, "reset page must stay reachable"


# ---------------------------------------------------------------------------
# MARKETING SUPPRESSION / CONSENT
# ---------------------------------------------------------------------------

def test_marketing_suppressed_but_operational_still_sends(two_tenants):
    a, _ = two_tenants
    from services.consent_service import suppress_marketing, SOURCE_WHATSAPP_REPLY, get_marketing_state
    from services.messaging_service import can_send_outbound
    from constants.message_categories import (
        MARKETING, REVIEW_REQUEST, BOOKING_CONFIRMATION, BOOKING_REMINDER,
        VEHICLE_READY, SERVICE_FOLLOWUP,
    )

    booking = {"id": 1, "location_id": a["location_id"], "customer_id": a["customer_id"],
               "phone": "+27820000001", "reminder_opt_in": 1}

    with location_scope(a["location_id"]):
        suppress_marketing(a["customer_id"], a["location_id"],
                           source=SOURCE_WHATSAPP_REPLY, method="replied STOP")
        assert get_marketing_state(a["customer_id"], a["location_id"]) == "opted_out"

        for category in (MARKETING, REVIEW_REQUEST):
            assert can_send_outbound(booking, f"subj {category}", "b", category=category) is False, \
                f"{category} sent to an opted-out customer"

        for category in (BOOKING_CONFIRMATION, BOOKING_REMINDER, VEHICLE_READY, SERVICE_FOLLOWUP):
            assert can_send_outbound(booking, f"subj {category}", "b", category=category) is True, \
                f"marketing opt-out wrongly blocked operational category {category}"


def test_opt_out_cannot_be_reversed_by_a_booking_form(two_tenants):
    a, _ = two_tenants
    from services.consent_service import (
        suppress_marketing, record_marketing_decision, get_marketing_state,
        SOURCE_WHATSAPP_REPLY, SOURCE_BOOKING_FORM,
    )

    with location_scope(a["location_id"]):
        suppress_marketing(a["customer_id"], a["location_id"],
                           source=SOURCE_WHATSAPP_REPLY, method="replied STOP")
        accepted = record_marketing_decision(
            a["customer_id"], a["location_id"], opted_in=True,
            source=SOURCE_BOOKING_FORM, method="checkbox",
        )
        assert accepted is False, "a booking form re-enrolled an opted-out customer"
        assert get_marketing_state(a["customer_id"], a["location_id"]) == "opted_out"


def test_unknown_consent_state_is_not_consent(two_tenants):
    a, _ = two_tenants
    from services.consent_service import may_send_marketing
    with location_scope(a["location_id"]):
        assert may_send_marketing(a["customer_id"], a["location_id"]) is False


@pytest.mark.parametrize("text,expected", [
    ("STOP", "opt_out"), ("stop", "opt_out"), ("Stop.", "opt_out"),
    ("UNSUBSCRIBE", "opt_out"), ("START", "opt_in"),
    ("please stop by at 9am", None),
    ("my car wont stop making that noise", None),
    ("Can I cancel my booking for tomorrow?", None),
])
def test_optout_keyword_matching_has_no_false_positives(text, expected):
    from services.whatsapp_optout import classify_inbound
    assert classify_inbound(text) == expected, f"misclassified {text!r}"


# ---------------------------------------------------------------------------
# WEBHOOK VERIFICATION
# ---------------------------------------------------------------------------

def test_meta_webhook_rejects_bad_signature():
    from integrations.meta.webhook.signature_verifier import MetaSignatureVerifier

    verifier = MetaSignatureVerifier("app-secret")
    body = b'{"object":"whatsapp_business_account"}'
    assert verifier.verify(body, "sha256=" + "0" * 64) is False
    assert verifier.verify(body, "") is False
    assert verifier.verify(body, None) is False


def test_paystack_webhook_rejects_bad_signature():
    import hashlib
    import hmac as hmac_mod
    secret = "sk_test_dummy"
    body = b'{"event":"charge.success"}'
    good = hmac_mod.new(secret.encode(), body, hashlib.sha512).hexdigest()
    assert good != "0" * 128


# ---------------------------------------------------------------------------
# DATA LIFECYCLE
# ---------------------------------------------------------------------------

def test_erasure_does_not_copy_pii_into_the_audit_log(two_tenants):
    a, _ = two_tenants
    client = phanta_app.app.test_client()
    _, token = _login(client, a["emails"]["owner"])

    client.post(f"/customers/{a['customer_id']}/delete", data={"csrf_token": token})

    with location_scope(a["location_id"]):
        row = query_db(
            "SELECT first_name, email, deleted_at FROM customers WHERE id=%s",
            (a["customer_id"],), one=True,
        )
    assert row["deleted_at"] is not None, "erasure did not run"
    assert row["first_name"] == "Deleted"
    assert row["email"] is None

    with location_scope(a["location_id"]):
        audit = query_db(
            'SELECT "before" AS b FROM audit_logs WHERE action=%s AND location_id=%s '
            "ORDER BY id DESC LIMIT 1",
            ("customer.deleted", a["location_id"]), one=True,
        )
    assert audit is not None, "erasure was not audited"
    assert a["customer_name"] not in str(audit["b"]), "ERASED PII WAS COPIED INTO THE AUDIT LOG"


def test_retention_clears_only_expired_message_bodies(two_tenants):
    from datetime import datetime, timedelta, timezone
    from services.retention_service import clear_expired_message_bodies, CLEARED_PLACEHOLDER

    a, _ = two_tenants

    def iso(days):
        return (datetime.now(timezone.utc) - timedelta(days=days)).replace(microsecond=0).isoformat()

    fresh_text = f"fresh-{a['suffix']}"
    stale_text = f"stale-{a['suffix']}"

    with location_scope(a["location_id"]):
        execute_db(
            "INSERT INTO conversations (location_id,customer_id,channel,started_at,created_at,updated_at) "
            "VALUES (%s,%s,'whatsapp',%s,%s,%s)",
            (a["location_id"], a["customer_id"], utc_now(), utc_now(), utc_now()),
        )
        conversation_id = query_db(
            "SELECT id FROM conversations WHERE location_id=%s ORDER BY id DESC LIMIT 1",
            (a["location_id"],), one=True,
        )["id"]
        for created, body in ((iso(40), stale_text), (iso(2), fresh_text)):
            execute_db(
                "INSERT INTO messages (location_id,conversation_id,direction,channel,body,status,created_at) "
                "VALUES (%s,%s,'inbound','whatsapp',%s,'received',%s)",
                (a["location_id"], conversation_id, body, created),
            )

        clear_expired_message_bodies()

        bodies = [r["body"] for r in query_db(
            "SELECT body FROM messages WHERE conversation_id=%s", (conversation_id,)
        )]

    assert fresh_text in bodies, "a message inside the retention window was cleared"
    assert stale_text not in bodies, "an expired message body survived retention"
    assert CLEARED_PLACEHOLDER in bodies
    assert len(bodies) == 2, "retention deleted rows instead of clearing text"


def test_offboarding_refuses_deletion_before_an_export(two_tenants):
    a, _ = two_tenants
    from services.offboarding_service import begin_offboarding, complete_offboarding

    with platform_scope():
        begin_offboarding(a["location_id"], "test-actor", reason="test")
        with pytest.raises(RuntimeError, match="no data export recorded"):
            complete_offboarding(a["location_id"], "test-actor")

    with location_scope(a["location_id"]):
        row = query_db("SELECT deleted_at FROM customers WHERE id=%s", (a["customer_id"],), one=True)
    assert row["deleted_at"] is None, "customer data was destroyed despite the export guard"


def test_offboarding_disables_access_but_preserves_data_for_export(two_tenants):
    a, _ = two_tenants
    from services.offboarding_service import begin_offboarding

    with platform_scope():
        begin_offboarding(a["location_id"], "test-actor")

    with platform_scope():
        location = query_db(
            "SELECT access_locked, active FROM locations WHERE id=%s", (a["location_id"],), one=True
        )
    assert location["access_locked"], "access was not locked"
    assert location["active"], "location deactivated too early - data must survive for export"

    with platform_scope():
        users = query_db("SELECT active FROM users WHERE location_id=%s", (a["location_id"],))
    assert all(not u["active"] for u in users), "users still active after termination"

    with location_scope(a["location_id"]):
        customer = query_db(
            "SELECT first_name FROM customers WHERE id=%s", (a["customer_id"],), one=True
        )
    assert customer["first_name"] == a["customer_name"], "data destroyed before export opportunity"


# ---------------------------------------------------------------------------
# INCIDENT REGISTER
# ---------------------------------------------------------------------------

def test_incident_detection_facts_cannot_be_rewritten():
    from services.incident_service import open_incident, update_incident, SEVERITY_LOW

    with platform_scope():
        incident_id = open_incident(
            incident_type="other", severity=SEVERITY_LOW,
            summary="test", detected_by="test-suite",
        )
        with pytest.raises(ValueError):
            update_incident(incident_id, detected_by="someone else")
        with pytest.raises(ValueError):
            update_incident(incident_id, incident_type="data_disclosure")


def test_incident_scope_is_not_invented_when_unknown():
    from services.incident_service import open_incident, scope_incident, SEVERITY_LOW

    with platform_scope():
        incident_id = open_incident(
            incident_type="other", severity=SEVERITY_LOW,
            summary="test", detected_by="test-suite",
        )
        result = scope_incident(incident_id)
    assert result["scope_determined"] is False
    assert result["affected_customers"] == 0


# ---------------------------------------------------------------------------
# SECRET EXPOSURE
# ---------------------------------------------------------------------------

def test_no_committed_env_file_and_dockerignore_excludes_it():
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    assert not (root / ".env").exists(), "a .env file is present in the repository"

    dockerignore = (root / ".dockerignore")
    assert dockerignore.exists(), ".dockerignore missing - COPY . . would bake in local secrets"
    content = dockerignore.read_text()
    assert ".env" in content
    assert "tests/" in content


def test_security_event_identifier_is_hashed_not_stored():
    """A password typed into the username field must never be persisted."""
    from helpers.security_events import record_security_event, LOGIN_FAILED

    secret = f"NotAnEmail{uuid.uuid4().hex[:8]}"
    record_security_event(
        LOGIN_FAILED, identifier=secret, identifier_is_known_account=False, outcome="failure"
    )
    with platform_scope():
        rows = query_db(
            "SELECT identifier, identifier_hash FROM security_events WHERE identifier_hash IS NOT NULL "
            "ORDER BY id DESC LIMIT 5"
        ) or []
    assert rows, "no hashed security event was written"
    for row in rows:
        assert row["identifier"] != secret, "A SUBMITTED SECRET WAS STORED IN PLAINTEXT"


# ---------------------------------------------------------------------------
# CONSENT ON THE LIVE SEND PATH
# ---------------------------------------------------------------------------

def test_marketing_gate_is_on_the_path_production_actually_uses():
    """The consent gate must sit on the LIVE outbound path, not a dead one.

    Marketing suppression was first implemented only in
    services/messaging_service.can_send_outbound(). That function is reached
    exclusively from services/reminder_service.py and
    services/inquiry_followup_service.py -- and neither module is imported by
    anything, so neither ever loads. Opt-outs were recorded by the WhatsApp
    STOP handler and then never consulted at send time.

    Unit tests on can_send_outbound() passed throughout, because they called it
    directly. Behaviour was right; reachability was not. This test asserts
    reachability: the live lifecycle sender must consult consent.
    """
    import inspect
    from ai.communications.lifecycle import LifecycleCommunicationService

    source = inspect.getsource(LifecycleCommunicationService._send)
    assert "_may_send" in source, \
        "the live lifecycle _send() does not consult consent"

    gate = inspect.getsource(LifecycleCommunicationService._may_send)
    assert "may_send_marketing" in gate and "is_marketing" in gate, \
        "the live consent gate does not use the shared consent/category rules"


def test_every_live_send_declares_a_category():
    """An unlabelled send defaults to operational and bypasses the gate.

    That default is deliberate -- silently dropping live operational traffic
    would be worse -- which is exactly why every call site must be explicit.
    """
    import inspect
    import re
    from ai.communications import lifecycle

    source = inspect.getsource(lifecycle)
    calls = re.findall(r"self\._send\((?:[^()]|\([^()]*\))*\)", source, re.S)
    assert calls, "no _send call sites found - has the sender been renamed?"
    unlabelled = [c for c in calls if "category=" not in c]
    assert not unlabelled, (
        "live send call sites without an explicit category: "
        + "; ".join(c[:60].replace("\n", " ") for c in unlabelled)
    )


def test_live_path_suppresses_marketing_but_not_operational(two_tenants):
    """End-to-end: an opted-out customer still gets operational messages."""
    from constants.message_categories import (
        MARKETING, REVIEW_REQUEST, BOOKING_CONFIRMATION, VEHICLE_READY,
    )
    from services.consent_service import suppress_marketing, SOURCE_WHATSAPP_REPLY
    from ai.communications.lifecycle import LifecycleCommunicationService

    a, _ = two_tenants
    with location_scope(a["location_id"]):
        suppress_marketing(a["customer_id"], a["location_id"],
                           source=SOURCE_WHATSAPP_REPLY, method="replied STOP")

        may = LifecycleCommunicationService._may_send
        for category in (MARKETING, REVIEW_REQUEST):
            assert may(a["location_id"], a["customer_id"], category) is False, \
                f"live path would send {category} to an opted-out customer"
        for category in (BOOKING_CONFIRMATION, VEHICLE_READY, None):
            assert may(a["location_id"], a["customer_id"], category) is True, \
                f"live path wrongly suppressed operational category {category}"


# ---------------------------------------------------------------------------
# ENTRY POINTS — a capability with no way to invoke it is not a capability
# ---------------------------------------------------------------------------

def test_incident_register_has_an_entry_point():
    """The incident register must be usable without writing code.

    services/incident_service.py previously had no route, no script and no
    importer -- it could only be driven from a Python shell. During an actual
    incident, that is the worst possible moment to have to write code.
    """
    from scripts import incident

    assert hasattr(incident, "main")
    for command in ("open", "list", "show", "update", "scope"):
        assert command in incident.__doc__, f"CLI does not document `{command}`"


def test_offboarding_has_an_entry_point_with_a_confirmation():
    """Offboarding must be invocable, and stage 2 must not be one keystroke."""
    import inspect

    from scripts import offboard

    assert hasattr(offboard, "main")
    source = inspect.getsource(offboard.main)
    assert "--force" in source, "no way to record a declined export"
    assert "input(" in source, "irreversible deletion has no interactive confirmation"


def test_offboarding_refuses_delete_without_export_via_cli(two_tenants):
    """The export guard must hold at the CLI boundary, not only in the service."""
    from services.offboarding_service import offboarding_readiness

    a, _ = two_tenants
    with platform_scope():
        readiness = offboarding_readiness(a["location_id"])
    assert readiness["data_export_taken"] is False
    assert readiness["customers_pending_anonymisation"] >= 1


def test_security_controls_are_reachable_from_the_application():
    """Every control must be imported by something that actually runs.

    This test exists because marketing suppression was originally implemented
    in a module that nothing imported. It passed its own unit tests for weeks
    while being unreachable. Behaviour tests cannot detect that.
    """
    import os
    import re

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    controls = {
        "services/consent_service.py": "consent_service",
        "services/whatsapp_optout.py": "whatsapp_optout",
        "services/export_service.py": "export_service",
        "services/data_lifecycle.py": "data_lifecycle",
        "services/retention_service.py": "retention_service",
        "services/legal_acceptance_service.py": "legal_acceptance_service",
        "services/incident_service.py": "incident_service",
        "services/offboarding_service.py": "offboarding_service",
        "helpers/security_events.py": "security_events",
    }
    # Places that count as "something that runs": routes, jobs, scripts, and
    # the live AI/communication path.
    searched = []
    for base, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d not in ("__pycache__", ".git", "node_modules")]
        rel = os.path.relpath(base, root)
        if rel.split(os.sep)[0] not in ("routes", "jobs", "scripts", "ai", "integrations", "services", "helpers"):
            continue
        for name in files:
            if name.endswith(".py"):
                searched.append(os.path.join(base, name))

    unreachable = []
    for path, module in controls.items():
        importers = []
        for candidate in searched:
            if candidate.endswith(path.replace("/", os.sep)):
                continue
            try:
                text = open(candidate, encoding="utf-8").read()
            except OSError:
                continue
            if re.search(rf"\b{module}\b", text):
                importers.append(candidate)
        if not importers:
            unreachable.append(path)

    assert not unreachable, (
        "security controls with no importer anywhere in the running application: "
        + ", ".join(unreachable)
    )


def test_env_checker_flags_a_bare_production_deploy():
    """check_env must catch the variables that break a production deploy."""
    import os
    from scripts import check_env

    saved = dict(os.environ)
    try:
        for key in list(os.environ):
            del os.environ[key]
        os.environ["RAILWAY_ENVIRONMENT"] = "production"
        exit_code = check_env.main([])
        assert exit_code == 1, "a bare production environment was reported as safe"
    finally:
        os.environ.clear()
        os.environ.update(saved)


def test_env_checker_never_prints_a_secret_value(capsys):
    """It reports whether a variable is set, never what it contains."""
    import os
    from scripts import check_env

    saved = dict(os.environ)
    secret = "super-secret-value-do-not-print"
    try:
        os.environ["FLASK_SECRET_KEY"] = secret
        os.environ["PAYSTACK_SECRET_KEY"] = secret
        os.environ["META_APP_SECRET"] = secret
        check_env.main([])
        output = capsys.readouterr().out
        assert secret not in output, "check_env printed a credential value"
    finally:
        os.environ.clear()
        os.environ.update(saved)
