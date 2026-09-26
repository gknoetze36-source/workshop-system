from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[2]


def test_dedicated_railway_cron_config_is_finite_and_scheduled():
    text = (ROOT / "railway-cron.toml").read_text()
    assert 'startCommand = "python -m jobs.scheduler"' in text
    assert 'cronSchedule = "*/5 * * * *"' in text


def test_web_service_does_not_run_the_cron_scheduler():
    # railway.toml's startCommand was refactored to indirect through
    # start.sh (for --bind/$PORT and --forwarded-allow-ips), so the
    # literal "gunicorn phanta_app:app" string moved out of railway.toml
    # itself. The property this test actually protects -- the web
    # service never also runs jobs.scheduler, which would double-execute
    # every job on top of the dedicated 5-minute cron service -- is
    # checked against wherever that command now actually lives.
    railway_toml = (ROOT / "railway.toml").read_text()
    start_sh = (ROOT / "start.sh").read_text()
    assert "cronSchedule" not in railway_toml
    assert "jobs.scheduler" not in railway_toml
    assert "jobs.scheduler" not in start_sh
    assert "gunicorn phanta_app:app" in railway_toml or "gunicorn phanta_app:app" in start_sh


def test_scheduler_registers_all_required_jobs():
    text = (ROOT / "jobs" / "scheduler.py").read_text()
    for name in (
        "run_meta_token_monitor",
        "run_lifecycle_communication",
        "run_follow_up_worker",
        "run_flyer_lady_publish_queue",
        "run_paystack_reconciliation",
    ):
        assert name in text


def test_location_owned_jobs_establish_location_scope():
    for filename in (
        "jobs/meta_token_monitor.py",
        "jobs/lifecycle_communication.py",
        "jobs/follow_up.py",
        "jobs/paystack_reconciliation.py",
    ):
        text = (ROOT / filename).read_text()
        assert "set_location_id" in text
        assert "Location.id" in text


def test_paystack_reconciliation_receives_location_scope():
    text = (ROOT / "jobs" / "paystack_reconciliation.py").read_text()
    assert "location_id=location_id" in text
    service_text = (ROOT / "integrations" / "paystack" / "reconciliation_service.py").read_text()
    assert "location_id" in service_text
    repo_text = (ROOT / "integrations" / "paystack" / "repositories" / "payment_repo.py").read_text()
    assert "Payment.location_id == location_id" in repo_text


def test_scheduler_is_valid_python_source():
    ast.parse((ROOT / "jobs" / "scheduler.py").read_text())
