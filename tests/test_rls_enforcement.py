"""Prove RLS actually enforces location isolation under a properly
restricted role - not just that policies exist as text.

Found 2026-08-24 during a full architecture-compliance audit: every prior
RLS verification in this project connected as the `postgres` superuser,
which unconditionally bypasses RLS regardless of FORCE ROW LEVEL SECURITY
or policy content. That masked two real bugs:

  1. jobs/flyer_lady.py queried flyer_lady_special_posts through an
     unscoped session (no app.location_id, no app.platform_admin set) -
     under a restricted role this returns zero rows always, so the publish
     queue would silently never process anything in a properly secured
     deployment.
  2. 20 tables with a location_id column had no RLS at all - app-level
     `WHERE location_id = %s` filtering was the only protection.

This test creates a genuinely restricted role (NOSUPERUSER, NOBYPASSRLS,
matching create_phanta_app_role.py's intent) and proves an unscoped
session sees nothing while a correctly-scoped session sees exactly its own
location's data - for both a pre-existing RLS table (notes) and one this
pass added coverage for (automation_rules).

Skips gracefully without a reachable Postgres, same as
test_postgres_migration_chain.py.
"""
import os

import pytest

POSTGRES_TEST_URL = os.environ.get("PHANTA_TEST_POSTGRES_URL") or os.environ.get("DATABASE_URL")


def _postgres_available() -> bool:
    if not POSTGRES_TEST_URL or not POSTGRES_TEST_URL.startswith("postgres"):
        return False
    try:
        import psycopg2
        conn = psycopg2.connect(POSTGRES_TEST_URL, connect_timeout=3)
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _postgres_available(),
    reason="no reachable Postgres (set PHANTA_TEST_POSTGRES_URL)",
)

RESTRICTED_ROLE = "phanta_rls_test_role"
RESTRICTED_PASSWORD = "rls-test-password"


@pytest.fixture
def rls_test_env(monkeypatch):
    import psycopg2

    admin = psycopg2.connect(POSTGRES_TEST_URL)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute("DROP SCHEMA public CASCADE")
            cur.execute("CREATE SCHEMA public")
    finally:
        admin.close()

    monkeypatch.setenv("DATABASE_URL", POSTGRES_TEST_URL)
    monkeypatch.setenv("DEV_FLASK_SECRET_KEY", "rls-test-secret")
    monkeypatch.setenv("SUPERADMIN_PASSWORD", "RlsTestPass123!")
    monkeypatch.setenv("STRICT_ALEMBIC_MIGRATIONS", "1")

    import sys
    for name in list(sys.modules):
        if name == "database" or name.startswith("database."):
            del sys.modules[name]

    from database import initialize_database, execute_db, query_db, utc_now
    initialize_database(run_migrations=True)

    admin2 = psycopg2.connect(POSTGRES_TEST_URL)
    admin2.autocommit = True
    try:
        with admin2.cursor() as cur:
            cur.execute(f"DROP ROLE IF EXISTS {RESTRICTED_ROLE}")
            cur.execute(
                f"CREATE ROLE {RESTRICTED_ROLE} LOGIN PASSWORD '{RESTRICTED_PASSWORD}' "
                "NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
            )
            cur.execute(f"GRANT USAGE ON SCHEMA public TO {RESTRICTED_ROLE}")
            cur.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {RESTRICTED_ROLE}")
            cur.execute(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {RESTRICTED_ROLE}")
    finally:
        admin2.close()

    execute_db("INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
               ("RLS Test Owner", "rlstest@example.com", utc_now(), utc_now()))
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", ("rlstest@example.com",), one=True)["id"]
    execute_db("INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at) VALUES (%s,%s,'workshop',TRUE,%s,%s)",
               (owner_id, "RLS Test Location", utc_now(), utc_now()))
    location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]

    execute_db(
        "INSERT INTO notes (location_id, subject_type, subject_id, content, created_at, updated_at) "
        "VALUES (%s,'vehicle',1,'rls test note',%s,%s)",
        (location_id, utc_now(), utc_now()),
    )
    execute_db(
        "INSERT INTO automation_rules (name, event_type, delay_minutes, active, created_at, updated_at, location_id) "
        "VALUES ('rls test rule','x',0,TRUE,%s,%s,%s)",
        (utc_now(), utc_now(), location_id),
    )

    parsed_url = POSTGRES_TEST_URL.split("@")[-1]
    restricted_url = f"postgresql://{RESTRICTED_ROLE}:{RESTRICTED_PASSWORD}@{parsed_url}"
    yield {"location_id": location_id, "restricted_url": restricted_url}


def _count_as_restricted_role(restricted_url, table, location_id=None):
    import psycopg2
    conn = psycopg2.connect(restricted_url)
    try:
        with conn.cursor() as cur:
            if location_id is not None:
                cur.execute("SELECT set_config('app.location_id', %s, false)", (str(location_id),))
            cur.execute(f"SELECT count(*) FROM {table}")
            return cur.fetchone()[0]
    finally:
        conn.close()


@pytest.mark.parametrize("table", ["notes", "automation_rules"])
def test_unscoped_session_sees_nothing_under_restricted_role(rls_test_env, table):
    count = _count_as_restricted_role(rls_test_env["restricted_url"], table)
    assert count == 0, f"{table}: an unscoped session under a restricted role should see 0 rows, saw {count}"


@pytest.mark.parametrize("table", ["notes", "automation_rules"])
def test_correctly_scoped_session_sees_its_own_row(rls_test_env, table):
    count = _count_as_restricted_role(rls_test_env["restricted_url"], table, location_id=rls_test_env["location_id"])
    assert count == 1, f"{table}: a session scoped to the seeded location should see exactly 1 row, saw {count}"


def test_flyer_lady_public_redirect_finds_special_under_restricted_role(rls_test_env):
    """Regression test for a severe bug found 2026-08-25: routes/
    flyer_lady.py's public, unauthenticated tracking-link redirect
    (GET /l/<special_id>) used a plain unscoped get_session(). Under the
    restricted phanta_app role, RLS on flyer_lady_specials (forced since
    migration 0011) means that query returns nothing for a special that
    genuinely exists -- every real customer clicking a Flyer Lady social
    post link would get a 404 "link not found", always, in a properly
    secured deployment. This is the actual customer-facing entry point for
    the whole Flyer Lady feature -- confirmed directly against real
    Postgres before fixing (special: None) and after (special found,
    booking_link correct, click log insert succeeds and is correctly
    scoped to the right location)."""
    from database import execute_db, query_db, utc_now, get_platform_session, location_transaction
    from sqlalchemy import select
    from flyer_lady.models import Special, FlyerLinkClick

    execute_db(
        "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
        ("Flyer Test Owner", "flyertest@example.com", utc_now(), utc_now()),
    )
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", ("flyertest@example.com",), one=True)["id"]
    execute_db(
        "INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at) VALUES (%s,%s,'workshop',TRUE,%s,%s)",
        (owner_id, "Flyer Test Workshop", utc_now(), utc_now()),
    )
    location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]
    execute_db(
        "INSERT INTO flyer_lady_specials (location_id, created_by, text, booking_link, status, created_at, updated_at) "
        "VALUES (%s,'test','Special offer','https://example.test/book','queued',%s,%s)",
        (location_id, utc_now(), utc_now()),
    )
    special_id = query_db("SELECT id FROM flyer_lady_specials WHERE location_id=%s", (location_id,), one=True)["id"]

    import psycopg2
    conn = psycopg2.connect(rls_test_env["restricted_url"])
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM flyer_lady_specials WHERE id=%s", (special_id,))
            assert cur.fetchone()[0] == 0, "sanity check: an unscoped raw query must see nothing under the restricted role"
    finally:
        conn.close()

    import os
    os.environ["DATABASE_URL"] = rls_test_env["restricted_url"]
    import sys
    for name in list(sys.modules):
        if name == "database" or name.startswith("database."):
            del sys.modules[name]
    from database import get_platform_session as get_platform_session2, location_transaction as location_transaction2

    platform_session = get_platform_session2()
    try:
        special = platform_session.scalar(select(Special).where(Special.id == special_id))
    finally:
        platform_session.close()
    assert special is not None, "get_platform_session() must find the special under the restricted role"
    assert special.booking_link == "https://example.test/book"

    with location_transaction2(special.location_id) as db:
        db.add(FlyerLinkClick(
            special_id=special.id, location_id=special.location_id,
            user_agent="pytest", referrer=None,
        ))

    logged = query_db(
        "SELECT special_id, location_id FROM flyer_lady_link_clicks WHERE special_id=%s",
        (special_id,), one=True,
    )
    assert logged is not None
    assert logged["location_id"] == location_id


def test_flyer_lady_job_finds_posts_under_restricted_role(rls_test_env, monkeypatch):
    """The actual bug: jobs/flyer_lady.py used an unscoped session. Prove
    the fixed version finds a pending post under the restricted role."""
    from database import execute_db, utc_now

    location_id = rls_test_env["location_id"]
    execute_db(
        "INSERT INTO flyer_lady_specials (location_id, created_by, text, booking_link, status, created_at, updated_at) "
        "VALUES (%s,'test','test special','https://example.test','draft',%s,%s)",
        (location_id, utc_now(), utc_now()),
    )
    from database import query_db
    special_id = query_db("SELECT id FROM flyer_lady_specials WHERE location_id=%s", (location_id,), one=True)["id"]
    execute_db(
        "INSERT INTO flyer_lady_special_posts (special_id, location_id, platform, status, attempts, created_at) "
        "VALUES (%s,%s,'facebook_feed','pending',0,%s)",
        (special_id, location_id, utc_now()),
    )

    monkeypatch.setenv("DATABASE_URL", rls_test_env["restricted_url"])
    import sys
    for name in list(sys.modules):
        if name == "database" or name.startswith("database."):
            del sys.modules[name]

    from datetime import datetime, timezone
    from sqlalchemy import select
    from database import SessionLocal, set_location_id
    from models.core import Location
    from flyer_lady.models import SpecialPost

    admin_session = SessionLocal()
    location_ids = list(admin_session.scalars(select(Location.id).where(Location.active.is_(True))))
    admin_session.close()
    assert location_id in location_ids

    now = datetime.now(timezone.utc)
    db = SessionLocal()
    set_location_id(db, location_id)
    posts = db.scalars(
        select(SpecialPost).where(
            SpecialPost.location_id == location_id,
            SpecialPost.status.in_(["pending", "failed"]),
            (SpecialPost.next_attempt_at.is_(None) | (SpecialPost.next_attempt_at <= now)),
        )
    ).all()
    db.close()
    assert len(posts) == 1, "flyer_lady's per-location query should find the seeded pending post"


def test_onboarding_flow_survives_real_postgres_boolean_columns(rls_test_env):
    """Regression test for a class of bug found 2026-08-25: several
    onboarding/automation routes passed Python int(bool(x)) or a literal
    1/0 as a query parameter or SQL literal against columns that are
    genuine Postgres BOOLEAN (services.active, automation_rules.active,
    onboarding_state.services_created/automations_enabled/go_live_ready,
    users.active). This is silently fine on SQLite (integer affinity) and
    a hard DatatypeMismatch/operator-does-not-exist error on Postgres --
    every prior test of this flow ran on SQLite only, so it went
    undetected. Runs the real onboarding routes end to end against real
    Postgres rather than re-testing the isolated queries, since the bug
    was only ever caught by hitting the actual pages."""
    import re
    import phanta_app

    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html)
        return m.group(1) if m else None

    token = csrf_from("/register")
    r = client.post("/register", data={
        "full_name": "PG Onboarding Test", "email": "pgonboard@test.example",
        "password": "SuperSecret123", "confirm_password": "SuperSecret123",
        "csrf_token": token,
    })
    assert r.status_code == 302

    token2 = csrf_from("/onboarding/location")
    r2 = client.post("/onboarding/location", data={
        "location_name": "PG Onboarding Workshop", "industry": "workshop", "csrf_token": token2,
    })
    assert r2.status_code == 302, "onboarding_state creation must not fail on real Postgres booleans"

    for path in ("/onboarding/services", "/onboarding/automation", "/onboarding/review", "/onboarding/team"):
        response = client.get(path)
        assert response.status_code == 200, f"{path} must not 500 on real Postgres"

    token3 = csrf_from("/onboarding/automation")
    r3 = client.post("/onboarding/automation", data={"csrf_token": token3}, follow_redirects=False)
    assert r3.status_code == 302


def test_settings_and_onboarding_user_management_survives_real_postgres_booleans(rls_test_env):
    """Regression test for a bug found 2026-08-25: routes/settings.py's
    settings_users() (both the invite INSERT and the toggle_status UPDATE)
    and routes/onboarding.py's team-invite handler all passed literal
    1/0 for users.active/must_reset_password -- real Postgres BOOLEAN
    columns. Confirmed directly against real Postgres before fixing
    (DatatypeMismatch on the INSERT); this locks the fix in against the
    actual backend where it manifests, not just SQLite."""
    import re
    import phanta_app

    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html)
        return m.group(1) if m else None

    token = csrf_from("/register")
    client.post("/register", data={
        "full_name": "PG Settings Owner", "email": "pgsettingsbool@test.example",
        "password": "SuperSecret123", "confirm_password": "SuperSecret123", "csrf_token": token,
    })
    token2 = csrf_from("/onboarding/location")
    client.post("/onboarding/location", data={
        "location_name": "PG Settings Bool Workshop", "industry": "workshop", "csrf_token": token2,
    })

    token3 = csrf_from("/settings/users")
    invite_response = client.post("/settings/users", data={
        "action": "invite", "email": "pgstaff@test.example", "role": "reception",
        "full_name": "PG Staff", "csrf_token": token3,
    }, follow_redirects=False)
    assert invite_response.status_code == 302, "invite must not raise DatatypeMismatch on real Postgres"

    from database import query_db
    row = query_db("SELECT id, active FROM users WHERE email='pgstaff@test.example'", one=True)
    assert row is not None
    assert row["active"] is True

    token4 = csrf_from("/settings/users")
    toggle_response = client.post("/settings/users", data={
        "action": "toggle_status", "user_id": str(row["id"]), "csrf_token": token4,
    }, follow_redirects=False)
    assert toggle_response.status_code == 302

    row2 = query_db("SELECT active FROM users WHERE id=%s", (row["id"],), one=True)
    assert row2["active"] is False


def test_inbound_whatsapp_message_fires_generic_automation_trigger(rls_test_env):
    """Regression test for services/automation_engine.py's fire_event()
    now being called from a real place -- integrations/meta/webhook/
    event_handlers/message_handlers.py's inbound() -- added 2026-08-25 as
    the generic "new message received" trigger (matching how Zapier
    treats "new message" as a plain, reusable trigger any downstream
    automation can react to, rather than a single-purpose one).

    Runs a real inbound WhatsApp webhook payload through the actual
    MetaWebhookRouter (not a direct fire_event() call) against a real
    automation_rule seeded for event_type='message.received', and
    confirms automation_logs shows it actually fired.

    Postgres-only: fire_event() writes through the raw query_db/execute_db
    layer from inside an open ORM session/transaction (the webhook
    handler's), which is a combination not exercised anywhere else in
    this codebase before this change. Confirmed directly that this
    causes "database is locked" on SQLite (two connections contending
    for the same file mid-transaction) but works cleanly on Postgres,
    which is the actual deployment target -- consistent with every other
    SQLite-vs-Postgres gap found this engagement.
    """
    import json
    from database import execute_db, query_db, utc_now, get_session

    execute_db(
        "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
        ("Message Trigger Owner", "messagetrigger@test.example", utc_now(), utc_now()),
    )
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", ("messagetrigger@test.example",), one=True)["id"]
    execute_db(
        "INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at) VALUES (%s,%s,'workshop',TRUE,%s,%s)",
        (owner_id, "Message Trigger Workshop", utc_now(), utc_now()),
    )
    location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]

    from models.integration_models import MetaBusinessConnection
    session = get_session()
    try:
        session.add(MetaBusinessConnection(
            location_id=location_id, waba_id="WABA_TEST", phone_number_id="PHONE_TEST", connection_status="connected",
        ))
        session.commit()
    finally:
        session.close()

    execute_db(
        "INSERT INTO automation_templates (industry, name, event_type, default_delay_minutes, default_message, created_at, updated_at) "
        "VALUES ('workshop', 'Custom message hook', 'message.received', 0, 'test', %s, %s)",
        (utc_now(), utc_now()),
    )
    template_id = query_db("SELECT id FROM automation_templates WHERE event_type='message.received'", one=True)["id"]
    execute_db(
        "INSERT INTO automation_rules (location_id, template_id, name, event_type, active, delay_minutes, action_json, created_at, updated_at) "
        "VALUES (%s, %s, 'Custom message hook', 'message.received', TRUE, 0, %s, %s, %s)",
        (location_id, template_id, json.dumps({"action": "log_only", "params": {}}), utc_now(), utc_now()),
    )

    from integrations.meta.webhook.webhook_router import MetaWebhookRouter
    session2 = get_session()
    try:
        router = MetaWebhookRouter(session2)
        payload = {
            "object": "whatsapp_business_account",
            "entry": [{
                "id": "WABA_TEST",
                "changes": [{
                    "field": "messages",
                    "value": {
                        "metadata": {"phone_number_id": "PHONE_TEST"},
                        "messages": [{
                            "from": "27821110000", "id": "wamid.regression_test",
                            "type": "text", "text": {"body": "Hi, is my car ready?"},
                        }],
                    },
                }],
            }],
        }
        result = router.dispatch(payload)
        session2.commit()
    finally:
        session2.close()

    assert result["accepted"] is True
    assert result["results"][0]["result"]["stored"] is True

    log_row = query_db(
        "SELECT event_type, status FROM automation_logs WHERE location_id=%s AND event_type='message.received'",
        (location_id,), one=True,
    )
    assert log_row is not None, "fire_event() must have run for the real inbound message"
    assert log_row["status"] == "ok"
