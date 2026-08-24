"""Regression test for services/automation_engine.py writing to
automation_logs and scheduled_jobs without location_id.

Found 2026-08-24 running the automation engine against a real Postgres
database for the first time (migration 0020_automation_location_ownership
correctly makes automation_rules/scheduled_jobs/automation_logs/failed_jobs
location-owned with location_id NOT NULL) -- SQLite's raw bootstrap version
of these tables didn't enforce NOT NULL the same way, so this had passed
every SQLite-based test in this suite despite being broken on the actual
production database backend.

_execute_action() and _schedule_action() both had location_id available as
a parameter but never included it in their INSERTs.
"""
from database import execute_db, query_db, utc_now, initialize_database
from services.automation_engine import fire_event, process_due_automation_jobs
import json


def setup_module(module):
    initialize_database(run_migrations=False)


def _make_owner_and_location(session_prefix):
    execute_db(
        "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s, %s, TRUE, %s, %s)",
        (f"{session_prefix} Owner", f"{session_prefix}@test.example", utc_now(), utc_now()),
    )
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", (f"{session_prefix}@test.example",), one=True)["id"]
    execute_db(
        "INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at) VALUES (%s, %s, 'workshop', TRUE, %s, %s)",
        (owner_id, f"{session_prefix} Workshop", utc_now(), utc_now()),
    )
    return query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]


def test_immediate_action_log_includes_location_id():
    location_id = _make_owner_and_location("automation_engine_immediate")
    execute_db(
        """INSERT INTO automation_rules (name, event_type, conditions_json, action_json, delay_minutes, active, created_at, updated_at, location_id)
           VALUES (%s, %s, NULL, %s, 0, TRUE, %s, %s, %s)""",
        ("immediate rule", "test_event", json.dumps({"action": "log_only", "params": {}}),
         utc_now(), utc_now(), location_id),
    )

    results = fire_event("test_event", location_id=location_id, context={})
    assert results[0]["outcome"]["status"] == "ok"

    log_row = query_db(
        "SELECT location_id FROM automation_logs WHERE location_id=%s ORDER BY id DESC LIMIT 1",
        (location_id,), one=True,
    )
    assert log_row is not None, "automation_logs row was not written"
    assert log_row["location_id"] == location_id


def test_delayed_action_scheduled_job_includes_location_id():
    location_id = _make_owner_and_location("automation_engine_delayed")
    execute_db(
        """INSERT INTO automation_rules (name, event_type, conditions_json, action_json, delay_minutes, active, created_at, updated_at, location_id)
           VALUES (%s, %s, NULL, %s, 30, TRUE, %s, %s, %s)""",
        ("delayed rule", "test_event_delayed", json.dumps({"action": "log_only", "params": {}}),
         utc_now(), utc_now(), location_id),
    )

    results = fire_event("test_event_delayed", location_id=location_id, context={"x": 1})
    assert results[0]["outcome"] == "scheduled"

    job_row = query_db(
        "SELECT id, location_id, status FROM scheduled_jobs WHERE job_type='automation_action' AND location_id=%s ORDER BY id DESC LIMIT 1",
        (location_id,), one=True,
    )
    assert job_row is not None, "scheduled_jobs row was not written"
    assert job_row["location_id"] == location_id

    # backdate and process, confirming the whole delayed path completes too
    execute_db("UPDATE scheduled_jobs SET scheduled_for=%s WHERE id=%s", ("2020-01-01T00:00:00", job_row["id"]))
    outcomes = process_due_automation_jobs()
    matching = [o for o in outcomes if o["job_id"] == job_row["id"]]
    assert matching and matching[0]["outcome"]["status"] == "ok"
