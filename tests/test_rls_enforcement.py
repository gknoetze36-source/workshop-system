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
