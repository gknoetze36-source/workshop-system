import json
from datetime import datetime, timedelta

from database import execute_db, utc_now
from platform_helpers import boolish, fetch_all, fetch_one
from platform_messaging import build_booking_confirmation_message, send_cheapest_message


def _json_loads(value, default=None):
    if not value:
        return default if default is not None else {}
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default if default is not None else {}


def _json_dumps(value):
    return json.dumps(value or {}, separators=(",", ":"), sort_keys=True)


def _iso_after(minutes):
    return (datetime.utcnow() + timedelta(minutes=int(minutes or 0))).replace(microsecond=0).isoformat()


def emit_event(franchise_id, event_type, payload=None):
    """Create scheduled jobs for active rules matching a tenant event."""
    if not franchise_id or not event_type:
        return 0

    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
    if not franchise or not boolish(franchise.get("active", 1)):
        return 0
    if not boolish(franchise.get("automation_enabled", 0)):
        return 0
    if (franchise.get("subscription_status") or "active").lower() not in {"active", "trialing"}:
        return 0

    rules = fetch_all(
        """
        SELECT ar.*, at.name AS template_name, at.default_delay_minutes, at.default_message
        FROM automation_rules ar
        LEFT JOIN automation_templates at ON at.id = ar.template_id
        WHERE ar.franchise_id=%s
          AND ar.event_type=%s
          AND COALESCE(ar.active, 1)=1
        ORDER BY ar.id
        """,
        (franchise_id, event_type),
    )
    created = 0
    for rule in rules:
        action = _json_loads(rule.get("action_json"), {})
        job_type = action.get("job_type") or _default_job_type(event_type)
        scheduled_for = _iso_after(rule.get("delay_minutes") or rule.get("default_delay_minutes") or 0)
        job_payload = {
            "event_type": event_type,
            "rule_id": rule["id"],
            "template_id": rule.get("template_id"),
            "template_name": rule.get("template_name"),
            "payload": payload or {},
            "action": action,
        }
        execute_db(
            """
            INSERT INTO scheduled_jobs (
                franchise_id, automation_rule_id, job_type, payload_json, scheduled_for,
                status, attempts, max_attempts, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, 'pending', 0, 3, %s, %s)
            """,
            (franchise_id, rule["id"], job_type, _json_dumps(job_payload), scheduled_for, utc_now(), utc_now()),
        )
        _log(franchise_id, rule["id"], None, event_type, "scheduled", f"Scheduled {job_type}")
        created += 1
    return created


def _default_job_type(event_type):
    if event_type == "booking.created":
        return "booking_confirmation"
    return "automation_log"


def process_due_jobs(limit=50):
    jobs = fetch_all(
        """
        SELECT sj.*, ar.event_type
        FROM scheduled_jobs sj
        LEFT JOIN automation_rules ar ON ar.id = sj.automation_rule_id
        WHERE sj.status='pending'
          AND sj.scheduled_for <= %s
        ORDER BY sj.scheduled_for ASC, sj.id ASC
        LIMIT %s
        """,
        (utc_now(), limit),
    )
    processed = 0
    for job in jobs:
        execute_db(
            "UPDATE scheduled_jobs SET status='running', locked_at=%s, attempts=COALESCE(attempts, 0) + 1, updated_at=%s WHERE id=%s AND status='pending'",
            (utc_now(), utc_now(), job["id"]),
        )
        try:
            _execute_job(job)
            execute_db("UPDATE scheduled_jobs SET status='completed', completed_at=%s, updated_at=%s WHERE id=%s", (utc_now(), utc_now(), job["id"]))
            _log(job["franchise_id"], job.get("automation_rule_id"), job["id"], job.get("event_type"), "completed", f"Completed {job['job_type']}")
            processed += 1
        except Exception as exc:
            attempts = int(job.get("attempts") or 0) + 1
            status = "failed" if attempts >= int(job.get("max_attempts") or 3) else "pending"
            execute_db(
                "UPDATE scheduled_jobs SET status=%s, last_error=%s, updated_at=%s WHERE id=%s",
                (status, str(exc), utc_now(), job["id"]),
            )
            _log(job["franchise_id"], job.get("automation_rule_id"), job["id"], job.get("event_type"), status, str(exc))
            if status == "failed":
                execute_db(
                    "INSERT INTO failed_jobs (franchise_id, scheduled_job_id, error_message, payload_json, failed_at, resolved) VALUES (%s, %s, %s, %s, %s, 0)",
                    (job["franchise_id"], job["id"], str(exc), job.get("payload_json") or "", utc_now()),
                )
    return processed


def _execute_job(job):
    payload = _json_loads(job.get("payload_json"), {})
    if job["job_type"] == "booking_confirmation":
        booking_reference = (payload.get("payload") or {}).get("booking_reference")
        if not booking_reference:
            raise ValueError("booking_reference missing from job payload")
        booking = fetch_one(
            """
            SELECT b.*, f.name AS franchise_name, br.name AS branch_name
            FROM bookings b
            LEFT JOIN franchises f ON f.id = b.franchise_id
            LEFT JOIN branches br ON br.id = b.branch_id
            WHERE b.booking_reference=%s
            """,
            (booking_reference,),
        )
        if not booking:
            raise ValueError(f"Booking {booking_reference} not found")
        subject, body = build_booking_confirmation_message(booking)
        send_cheapest_message(booking, subject, body)
        return
    if job["job_type"] == "automation_log":
        return
    raise ValueError(f"Unknown job_type: {job['job_type']}")


def _log(franchise_id, rule_id, job_id, event_type, status, message):
    execute_db(
        """
        INSERT INTO automation_logs (
            franchise_id, automation_rule_id, scheduled_job_id, event_type, status, message, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (franchise_id, rule_id, job_id, event_type, status, message, utc_now()),
    )
