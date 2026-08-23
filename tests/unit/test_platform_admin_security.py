from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

def test_platform_admin_migration_is_select_only():
    text = (ROOT / "migrations/versions/0012_platform_admin_read_policy.py").read_text()
    assert "FOR SELECT" in text
    assert "app.platform_admin" in text
    assert "FOR UPDATE" not in text
    assert "FOR DELETE" not in text

def test_json_requests_are_csrf_protected():
    text = (ROOT / "phanta_app.py").read_text()
    assert "WTF_CSRF_CHECK_DEFAULT'] = True" in text
    assert "and not request.is_json" not in text

def test_flyer_lady_sends_csrf_header():
    text = (ROOT / "templates/flyer_lady.html").read_text()
    assert "X-CSRFToken" in text

def test_production_container_runs_migrations_before_web():
    # Migrations run via Railway's preDeployCommand (database/predeploy.py),
    # not a command baked into the Dockerfile -- predeploy runs and
    # completes before the web process (the Dockerfile's CMD) ever starts,
    # so this is the actual mechanism to assert on rather than grepping the
    # Dockerfile for a step it was never responsible for.
    railway_config = (ROOT / "railway.toml").read_text()
    assert "preDeployCommand" in railway_config
    assert "database.predeploy" in railway_config

    predeploy_text = (ROOT / "database/predeploy.py").read_text()
    assert "initialize_database" in predeploy_text

    initialize_text = (ROOT / "database/initialize.py").read_text()
    assert "run_alembic_migrations" in initialize_text
