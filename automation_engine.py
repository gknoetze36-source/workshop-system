import json
from datetime import datetime, timedelta

from database import execute_db, utc_now
from platform_helpers import can_run_automation, fetch_all, fetch_one
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


def _render_template(text, context):
    rendered = text or ""
    values = {
        "business_name": context.get("franchise_name") or context.get("business_name") or "",
        "branch_name": context.get("branch_name") or "",
        "customer_name": context.get("first_name") or context.get("customer_name") or "Customer",
        "service": context.get("service") or "your booking",
        "booking_reference": context.get("booking_reference") or "",
        "scheduled_date": context.get("scheduled_date") or context.get("date") or "",
    }
    for key, value in values.items():
        rendered = rendered.replace("{" + key + "}", str(value or ""))
    return rendered


def _conditions_match(conditions, event_payload):
    if not conditions:
        return True
    for key, expected in conditions.items():
        actual = event_payload.get(key)
        if isinstance(expected, list):
            if actual not in expected:
                return False
        elif str(actual or "").lower() != str(expected or "").lower():
            return False
    return True


def emit_event(franchise_id, event_type, payload=None):
    """Create scheduled jobs for active rules matching a tenant event."""
    if not franchise_id or not event_type:
        return 0

    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
    if not can_run_automation(franchise):
        return 0

    rules = fetch_all(
        """
        SELECT ar.*, at.name AS template_name, at.default_delay_minutes, at.default_message
        FROM automation_rules ar
        LEFT JOIN automation_templates at ON at.id = ar.template_id
        WHERE ar.franchise_id=%s
          AND ar.event_type=%s
          AND COALESCE(ar.active, TRUE)=TRUE
        ORDER BY ar.id
        """,
        (franchise_id, event_type),
    )
    created = 0
    for rule in rules:
        if not _conditions_match(_json_loads(rule.get("conditions_json"), {}), payload or {}):
            continue
        action = _json_loads(rule.get("action_json"), {})
        job_type = action.get("job_type") or _default_job_type(event_type)
        scheduled_for = _iso_after(rule.get("delay_minutes") or rule.get("default_delay_minutes") or 0)
        job_payload = {
            "event_type": event_type,
            "rule_id": rule["id"],
            "template_id": rule.get("template_id"),
            "template_name": rule.get("template_name"),
            "default_message": rule.get("default_message"),
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
        return "send_message"
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
        locked_at = utc_now()
        execute_db(
            "UPDATE scheduled_jobs SET status='running', locked_at=%s, attempts=COALESCE(attempts, 0) + 1, updated_at=%s WHERE id=%s AND status='pending'",
            (locked_at, locked_at, job["id"]),
        )
        claimed = fetch_one("SELECT * FROM scheduled_jobs WHERE id=%s AND status='running' AND locked_at=%s", (job["id"], locked_at))
        if not claimed:
            continue
        job = {**job, **claimed}
        try:
            _execute_job(job)
            execute_db("UPDATE scheduled_jobs SET status='completed', completed_at=%s, updated_at=%s WHERE id=%s", (utc_now(), utc_now(), job["id"]))
            _log(job["franchise_id"], job.get("automation_rule_id"), job["id"], job.get("event_type"), "completed", f"Completed {job['job_type']}")
            processed += 1
        except Exception as exc:
            attempts = int(job.get("attempts") or 1)
            status = "failed" if attempts >= int(job.get("max_attempts") or 3) else "pending"
            retry_at = _iso_after(_retry_delay_minutes(attempts)) if status == "pending" else job.get("scheduled_for")
            execute_db(
                "UPDATE scheduled_jobs SET status=%s, scheduled_for=%s, last_error=%s, updated_at=%s WHERE id=%s",
                (status, retry_at, str(exc), utc_now(), job["id"]),
            )
            _log(job["franchise_id"], job.get("automation_rule_id"), job["id"], job.get("event_type"), status, str(exc))
            if status == "failed":
                execute_db(
                    "INSERT INTO failed_jobs (franchise_id, scheduled_job_id, error_message, payload_json, failed_at, resolved) VALUES (%s, %s, %s, %s, %s, 0)",
                    (job["franchise_id"], job["id"], str(exc), job.get("payload_json") or "", utc_now()),
                )
    return processed


def _retry_delay_minutes(attempts):
    return {1: 1, 2: 5, 3: 30}.get(int(attempts or 1), 60)


def _execute_job(job):
    payload = _json_loads(job.get("payload_json"), {})
    action = payload.get("action") or {}
    if job["job_type"] in {"booking_confirmation", "send_message"} or action.get("type") == "send_message":
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
        subject, body = _message_for_job(booking, payload)
        sent, channel = send_cheapest_message(booking, subject, body)
        if not sent and channel == "suppressed":
            raise ValueError("Outbound message suppressed by subscription, opt-in, duplicate, or recipient policy")
        return
    if job["job_type"] == "automation_log":
        return
    raise ValueError(f"Unknown job_type: {job['job_type']}")


def _message_for_job(booking, payload):
    action = payload.get("action") or {}
    if action.get("subject") or action.get("body") or payload.get("default_message"):
        subject = _render_template(action.get("subject") or payload.get("template_name") or "Booking update", booking)
        body = _render_template(action.get("body") or payload.get("default_message") or "", booking)
        return subject, body
    return build_booking_confirmation_message(booking)


def retry_failed_job(failed_job_id):
    failed = fetch_one("SELECT * FROM failed_jobs WHERE id=%s AND COALESCE(resolved, 0)=0", (failed_job_id,))
    if not failed:
        return False
    execute_db(
        "UPDATE scheduled_jobs SET status='pending', scheduled_for=%s, attempts=0, last_error=NULL, updated_at=%s WHERE id=%s",
        (utc_now(), utc_now(), failed["scheduled_job_id"]),
    )
    execute_db("UPDATE failed_jobs SET resolved=1, resolved_at=%s WHERE id=%s", (utc_now(), failed_job_id))
    return True


def _log(franchise_id, rule_id, job_id, event_type, status, message):
    execute_db(
        """
        INSERT INTO automation_logs (
            franchise_id, automation_rule_id, scheduled_job_id, event_type, status, message, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (franchise_id, rule_id, job_id, event_type, status, message, utc_now()),
    )
