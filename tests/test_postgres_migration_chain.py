"""Full Alembic migration chain, run against real Postgres.

Why this exists: the SQLite-backed boot smoke tests (test_boot_smoke.py)
never exercise Alembic at all -- run_alembic_migrations() only runs when
the backend is postgresql. That gap is exactly how migrations 0001-0015
were able to reference a `tenants` table and `tenant_id` columns that
haven't existed in the ORM models since the owner/location refactor,
completely undetected, for an unknown length of time: on a genuinely fresh
Postgres database, migration 0001 (the very first one) raised
`RuntimeError: RLS table customers has no tenant_id column` and halted the
entire chain. Discovered 2026-08-24 by actually installing Postgres and
running this end-to-end -- not by reading the code.

This test does the same thing on every run where a Postgres instance is
reachable: build a database from absolutely nothing, run every migration,
and confirm the chain completes and lands at head. Skips (not fails) when
no Postgres is available, matching test_postgres_phase2.py's pattern --
this is a real gap in what CI verifies if Postgres is never set up for it,
but at minimum it means this test is available to run deliberately before
a deploy, and will run automatically wherever a Postgres service is wired
into CI.

Requires PHANTA_TEST_POSTGRES_URL or DATABASE_URL pointing at a database
the test is allowed to drop and recreate. Never point this at anything
with real data.
"""
import os
import subprocess

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
    reason="no reachable Postgres (set PHANTA_TEST_POSTGRES_URL) - this test cannot run against SQLite",
)


@pytest.fixture
def fresh_postgres_env(monkeypatch):
    """Drop and recreate every table in the target database, then point
    the app's env vars at it. Requires the connecting role to be able to
    drop its own schema (true for a dedicated test database/role)."""
    import psycopg2

    conn = psycopg2.connect(POSTGRES_TEST_URL)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA public CASCADE")
            cur.execute("CREATE SCHEMA public")
    finally:
        conn.close()

    monkeypatch.setenv("DATABASE_URL", POSTGRES_TEST_URL)
    monkeypatch.setenv("DEV_FLASK_SECRET_KEY", "postgres-test-secret")
    monkeypatch.setenv("SUPERADMIN_PASSWORD", "PostgresTestPass123!")
    monkeypatch.setenv("STRICT_ALEMBIC_MIGRATIONS", "1")

    import sys
    for name in list(sys.modules):
        if name == "database" or name.startswith("database."):
            del sys.modules[name]

    yield POSTGRES_TEST_URL


def test_full_migration_chain_succeeds_from_empty_postgres(fresh_postgres_env):
    """The exact scenario that was broken: every migration, in order,
    against a database with nothing in it. Must reach head with no
    exception -- STRICT_ALEMBIC_MIGRATIONS=1 ensures a failure here raises
    instead of being silently swallowed the way it is by default."""
    from database import initialize_database

    assert initialize_database(run_migrations=True) is True

    import psycopg2
    conn = psycopg2.connect(fresh_postgres_env)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version_num FROM alembic_version")
            version = cur.fetchone()[0]
            assert version == "0020_automation_location_ownership"

            cur.execute("SELECT to_regclass('public.tenants')")
            assert cur.fetchone()[0] is None, "a 'tenants' table should never exist - the location model is canonical"

            cur.execute("SELECT to_regclass('public.notes')")
            assert cur.fetchone()[0] is not None, "notes table was not created"

            cur.execute("""
                SELECT pg_get_expr(polqual, polrelid) FROM pg_policy
                WHERE polrelid = 'notes'::regclass AND polname LIKE '%location%'
            """)
            policy_rows = cur.fetchall()
            assert policy_rows, "notes has no location-scoped RLS policy"
            assert any("app.location_id" in row[0] for row in policy_rows), \
                "notes RLS policy does not check app.location_id (the GUC the app actually sets)"
    finally:
        conn.close()


def test_migration_chain_is_idempotent_against_postgres(fresh_postgres_env):
    """Every real deploy re-runs this against an already-migrated database."""
    from database import initialize_database

    assert initialize_database(run_migrations=True) is True
    assert initialize_database(run_migrations=True) is True
