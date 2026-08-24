"""Boot smoke test.

This is the test that would have caught, on the very first run, every one of
the crash-on-deploy bugs found in the 2026-08-21 incident:
  - ensure_owner_location_foundation() called after _ensure_columns() instead
    of before it (locations table used before it existed)
  - a trailing comma in the locations CREATE TABLE (SQL syntax error)
  - a "locationes" typo table that _ensure_columns tried to ALTER but that
    was never created anywhere

None of those were syntax errors Python itself catches, and none were
covered by the existing unit tests (which check source text, not runtime
behaviour). This test instead does the simplest possible thing: build a
throwaway database from nothing, the same way a fresh Railway deploy does,
and confirm the app is actually able to come up and answer a request.

Run in CI (or locally) before every deploy:
    pytest tests/test_boot_smoke.py -v
"""
import importlib
import os
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def fresh_sqlite_env(tmp_path, monkeypatch):
    """Point the app at a brand-new, empty SQLite file and the minimal env
    vars phanta_app.py requires to boot, mirroring a from-scratch deploy."""
    db_path = tmp_path / f"phanta_smoke_{uuid.uuid4().hex}.db"
    monkeypatch.setenv("PHANTA_SQLITE_PATH", str(db_path))
    monkeypatch.delenv("DATABASE_URL", raising=False)
    for var in ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DEV_FLASK_SECRET_KEY", "smoke-test-secret")
    monkeypatch.setenv("SUPERADMIN_PASSWORD", "SmokeTestPassword123!")
    monkeypatch.delenv("FLASK_ENV", raising=False)
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.setenv("SKIP_ALEMBIC_MIGRATIONS", "1")  # SQLite path never runs Alembic anyway

    # Every database.* submodule may already be imported (with a stale
    # connection cached) by whatever ran before this test, so force a clean
    # reimport against the new env vars.
    #
    # This test's own body then does `import phanta_app`, which transitively
    # reimports the entire route/service/repository tree -- all of it ends
    # up bound to this test's tmp-path database, module-level engines and
    # PRIMARY_SQLITE_PATH included. Deleting only the `database`/`phanta_app`
    # prefixes and leaving everything else in place used to leave those
    # transitively-reimported modules (services.billing_service was one)
    # permanently bound to this tmp path for the rest of the pytest session
    # -- any later test file that happened to run after this one would
    # silently read/write the wrong (deleted) database, with no error, just
    # empty results. Snapshotting and restoring the whole of sys.modules
    # makes this test's disruption fully self-contained, the way a passing
    # fixture should behave, instead of leaking into every test that follows.
    modules_snapshot = dict(sys.modules)
    for name in list(sys.modules):
        if name == "database" or name.startswith("database.") or name == "phanta_app":
            del sys.modules[name]

    try:
        yield db_path
    finally:
        for name in list(sys.modules):
            if name not in modules_snapshot:
                del sys.modules[name]
        sys.modules.update(modules_snapshot)


def test_initialize_database_succeeds_from_empty(fresh_sqlite_env):
    """The core bootstrap must succeed against a database with zero tables --
    this is what Railway's preDeployCommand runs, and what phanta_app.py runs
    again on every worker boot."""
    from database import initialize_database

    assert initialize_database(run_migrations=False) is True


def test_app_imports_and_boots_from_empty_database(fresh_sqlite_env):
    """Import phanta_app the same way gunicorn does. If schema bootstrap is
    broken, this raises during import -- exactly how the real incident
    surfaced (a Railway container crash-looping on startup)."""
    import phanta_app

    assert phanta_app.app is not None


def test_health_endpoint_returns_ok_from_empty_database(fresh_sqlite_env):
    """End-to-end: boot the app, hit /health with the real Flask test client,
    same endpoint Railway's healthcheck polls after deploy."""
    import phanta_app

    client = phanta_app.app.test_client()
    response = client.get("/health")
    assert response.status_code == 200
    assert response.get_json()["status"] == "ok"


def test_initialize_database_is_idempotent(fresh_sqlite_env):
    """Every deploy re-runs this against a database that already has the
    schema from the previous deploy. It must be safe to run twice."""
    from database import initialize_database

    assert initialize_database(run_migrations=False) is True
    assert initialize_database(run_migrations=False) is True


def test_initialize_database_is_idempotent_with_an_existing_location(fresh_sqlite_env):
    """Every deploy after your first real signup re-runs this against a
    database that already has at least one location row -- this is the
    scenario _seed_plan_defaults() actually writes to (it UPDATEs existing
    locations, so it only runs its real code path once a location exists).
    A column referenced there but missing from the locations table (as
    user_limit was) passes every other boot check and only breaks here."""
    from database import initialize_database, execute_db, utc_now

    assert initialize_database(run_migrations=False) is True

    execute_db(
        "INSERT INTO owners (name, email, created_at, updated_at) VALUES (%s, %s, %s, %s)",
        ("Smoke Test Owner", "smoke@example.com", utc_now(), utc_now()),
    )
    execute_db(
        "INSERT INTO locations (owner_id, name, created_at, updated_at) VALUES (1, %s, %s, %s)",
        ("Smoke Test Workshop", utc_now(), utc_now()),
    )

    # This is the real assertion: re-running against a DB that already has
    # a location must not raise.
    assert initialize_database(run_migrations=False) is True


def test_predeploy_and_app_boot_use_the_same_bootstrap_path():
    """Guard against the two entrypoints silently diverging again --
    database/predeploy.py (Railway's preDeployCommand) and phanta_app.py
    (every gunicorn worker) must call the exact same initialize_database(),
    not two hand-maintained copies of the same table-creation logic."""
    predeploy_source = (ROOT / "database" / "predeploy.py").read_text()
    assert "initialize_database" in predeploy_source

    app_source = (ROOT / "phanta_app.py").read_text()
    assert "initialize_database" in app_source
