"""Generic trigger -> condition -> action automation engine for PHANTA.

Why this exists
----------------
automation_rules already has conditions_json and action_json columns, but
before this module nothing in the codebase read or wrote them: every
"automation" was a hardcoded event_type -> fixed message, toggled on/off per
location. This module makes those two columns real, so a rule can express
"when X happens, if Y, do Z" the way Zapier does, instead of every new
automation requiring a code change.

This is additive: it does not remove or change the existing hardcoded
lifecycle/follow-up jobs in jobs/. Those keep working. New automations can be
authored as data (rows in automation_rules) and fired with fire_event().

Usage
-----
Fire a business event from wherever it happens (a booking route, a webhook
handler, a scheduled job):

    from services.automation_engine import fire_event

    fire_event(
        "booking_completed",
        location_id=booking["location_id"],
        context={
            "booking": {"id": booking["id"], "status": booking["status"]},
            "customer_id": booking["customer_id"],
            "conversation_id": conversation_id,
        },
    )

fire_event() loads active automation_rules for that location + event_type
(the same lookup routes/automations.py already uses), evaluates each rule's
conditions_json against the context you passed in, and for every rule that
matches:
  - delay_minutes == 0  -> runs the action immediately, inline
  - delay_minutes > 0   -> inserts a scheduled_jobs row; the job runner
                            (process_due_automation_jobs, wired into
                            jobs/scheduler.py) executes it later

Rule shape (JSON stored in automation_rules.conditions_json / action_json)
----------------------------------------------------------------------
conditions_json: a list of conditions, ALL of which must pass. Empty/null
means "always match" (an unconditional rule):

    [
        {"field": "booking.status", "op": "eq", "value": "completed"},
        {"field": "customer.accepts_whatsapp", "op": "eq", "value": true}
    ]

`field` is a dot-path looked up against the context dict you passed to
fire_event(). Supported ops: eq, neq, gt, gte, lt, lte, contains, in.

action_json: {"action": "<name registered in ACTIONS>", "params": {...}}

Adding a new action (e.g. a Flyer Lady publish, a Service Advisor nudge) is
writing one function and registering it with @register_action("name") --
nothing else in this module changes. See the bottom of this file for the
two actions wired in so far, and why the rest were left as an extension
point rather than guessed at.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from database import execute_db, query_db, utc_now
from repositories.automation_repository import (
    get_automation_rules_by_location_and_event,
    get_pending_scheduled_jobs,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Condition evaluation
# ---------------------------------------------------------------------------

def _lookup(context: dict, dotted_path: str):
    value = context
    for part in dotted_path.split("."):
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return None
    return value


_OPS = {
    "eq": lambda a, b: a == b,
    "neq": lambda a, b: a != b,
    "gt": lambda a, b: a is not None and a > b,
    "gte": lambda a, b: a is not None and a >= b,
    "lt": lambda a, b: a is not None and a < b,
    "lte": lambda a, b: a is not None and a <= b,
    "contains": lambda a, b: (b in a) if a is not None else False,
    "in": lambda a, b: (a in b) if b is not None else False,
}


def evaluate_conditions(conditions, context: dict) -> bool:
    """Return True if every condition matches. Falsy conditions always match."""
    if not conditions:
        return True
    if isinstance(conditions, str):
        conditions = json.loads(conditions)
    for condition in conditions:
        field = condition.get("field")
        op = condition.get("op", "eq")
        expected = condition.get("value")
        actual = _lookup(context, field) if field else None
        comparator = _OPS.get(op)
        if comparator is None:
            logger.warning("automation_engine: unknown condition op %r", op)
            return False
        try:
            if not comparator(actual, expected):
                return False
        except TypeError:
            return False
    return True


# ---------------------------------------------------------------------------
# Action registry
# ---------------------------------------------------------------------------

ACTIONS = {}


def register_action(name):
    """Decorator: registers fn(location_id, context, params) as an action."""

    def decorator(fn):
        ACTIONS[name] = fn
        return fn

    return decorator


@register_action("log_only")
def _action_log_only(location_id, context, params):
    """No-op action for testing a rule's trigger/conditions before wiring a
    real side effect. Always succeeds."""
    logger.info(
        "automation_engine log_only: location=%s context=%s params=%s",
        location_id, context, params,
    )
    return {"logged": True}


@register_action("whatsapp_message")
def _action_whatsapp_message(location_id, context, params):
    """Send a WhatsApp message via the existing Meta messaging pipeline.

    Requires conversation_id and customer_id in the trigger context -- this
    action does not create or guess a conversation, it sends into one that
    already exists. `params["text"]` is the message body; {field.path}
    placeholders in it are filled from context (falls back to the literal
    template if a placeholder is missing rather than failing the job).
    """
    from database import SessionLocal
    from ai.service_advisor.runtime import deliver_whatsapp

    conversation_id = context.get("conversation_id")
    customer_id = context.get("customer_id")
    if not conversation_id or not customer_id:
        raise ValueError(
            "whatsapp_message action requires conversation_id and customer_id in context"
        )

    text = params.get("text", "")
    try:
        text = text.format(**context)
    except (KeyError, IndexError):
        pass

    session = SessionLocal()
    try:
        result = deliver_whatsapp(
            session,
            location_id=location_id,
            conversation_id=conversation_id,
            customer_id=customer_id,
            text=text,
        )
        session.commit()
        return result
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# Deliberately not wired yet: flyer_lady_publish and a service_advisor
# action. FlyerLadyPublishService.publish_post() needs an already-created
# SpecialPost row (special_id, platform, approval state) -- a generic
# trigger call doesn't have enough to construct one safely, and guessing at
# which platform/special to use would be inventing product behaviour. The
# Service Advisor's build_service_advisor() drives a full conversation
# loop, not a single fire-and-forget call. Both are one @register_action
# function away once you decide what a rule should actually pass them --
# e.g. action_json = {"action": "flyer_lady_publish", "params": {"post_id": ...}}
# for a rule that fires after a SpecialPost already exists.


# ---------------------------------------------------------------------------
# Trigger entry point
# ---------------------------------------------------------------------------

def fire_event(event_type: str, location_id: int, context: dict | None = None) -> list[dict]:
    """Evaluate every active rule for (location_id, event_type) against context.

    Matching rules with delay_minutes == 0 execute immediately; others are
    queued as a scheduled_jobs row for process_due_automation_jobs to pick
    up later. Returns one result dict per rule considered, for logging/tests.
    """
    context = context or {}
    rules = get_automation_rules_by_location_and_event(location_id, event_type)
    results = []
    for rule in rules:
        matched = evaluate_conditions(rule.get("conditions_json"), context)
        outcome = {"rule_id": rule["id"], "matched": matched}
        if not matched:
            results.append(outcome)
            continue

        delay_minutes = rule.get("delay_minutes") or 0
        if delay_minutes <= 0:
            outcome["outcome"] = _execute_action(rule, location_id, context)
        else:
            _schedule_action(rule, location_id, delay_minutes, context)
            outcome["outcome"] = "scheduled"
        results.append(outcome)
    return results


def _execute_action(rule, location_id, context):
    action_json = rule.get("action_json")
    if not action_json:
        _log_automation(rule, location_id, None, "error", "rule has no action_json")
        return {"status": "error", "error": "rule has no action_json"}

    spec = json.loads(action_json) if isinstance(action_json, str) else action_json
    action_name = spec.get("action")
    params = spec.get("params", {})
    fn = ACTIONS.get(action_name)
    if fn is None:
        message = f"unregistered action: {action_name}"
        _log_automation(rule, location_id, None, "error", message)
        return {"status": "error", "error": message}

    try:
        result = fn(location_id, context, params)
        _log_automation(rule, location_id, None, "ok", None)
        return {"status": "ok", "result": result}
    except Exception as exc:
        logger.exception(
            "automation_engine action failed: rule=%s action=%s", rule["id"], action_name
        )
        _log_automation(rule, location_id, None, "error", str(exc))
        return {"status": "error", "error": str(exc)}


def _schedule_action(rule, location_id, delay_minutes, context):
    scheduled_for = (datetime.utcnow() + timedelta(minutes=delay_minutes)).replace(microsecond=0).isoformat()
    payload = json.dumps({"context": context})
    execute_db(
        """
        INSERT INTO scheduled_jobs
            (automation_rule_id, job_type, payload_json, scheduled_for, status, created_at, updated_at, location_id)
        VALUES (%s, %s, %s, %s, 'pending', %s, %s, %s)
        """,
        (rule["id"], "automation_action", payload, scheduled_for, utc_now(), utc_now(), location_id),
    )


def _log_automation(rule, location_id, scheduled_job_id, status, message):
    execute_db(
        """
        INSERT INTO automation_logs
            (automation_rule_id, scheduled_job_id, event_type, status, message, created_at, location_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (rule["id"], scheduled_job_id, rule.get("event_type"), status, message, utc_now(), location_id),
    )


# ---------------------------------------------------------------------------
# Scheduled job processor -- wire into jobs/scheduler.py's run_scheduled_jobs()
# ---------------------------------------------------------------------------

def process_due_automation_jobs(limit: int = 50) -> list[dict]:
    """Execute due scheduled_jobs rows created by fire_event()'s delayed path.

    Only touches job_type == 'automation_action'; other job types in
    scheduled_jobs (if any exist) are left untouched.
    """
    jobs = [
        job for job in get_pending_scheduled_jobs(limit=limit)
        if job.get("job_type") == "automation_action"
    ]
    results = []
    for job in jobs:
        rule = query_db(
            "SELECT * FROM automation_rules WHERE id = %s",
            (job["automation_rule_id"],),
            one=True,
        )
        if not rule:
            _mark_job(job["id"], "failed", "automation_rule no longer exists")
            results.append({"job_id": job["id"], "outcome": {"status": "error"}})
            continue

        payload = json.loads(job["payload_json"]) if job.get("payload_json") else {}
        context = payload.get("context", {})
        location_id = rule["location_id"]

        _mark_job(job["id"], "running")
        outcome = _execute_action(rule, location_id, context)
        if outcome.get("status") == "ok":
            _mark_job(job["id"], "completed")
        else:
            _mark_job(job["id"], "failed", outcome.get("error"))
        results.append({"job_id": job["id"], "outcome": outcome})
    return results


def _mark_job(job_id, status, error=None):
    if status == "failed":
        execute_db(
            "UPDATE scheduled_jobs SET status=%s, last_error=%s, updated_at=%s WHERE id=%s",
            (status, error, utc_now(), job_id),
        )
        execute_db(
            "INSERT INTO failed_jobs (scheduled_job_id, error_message, failed_at) VALUES (%s, %s, %s)",
            (job_id, error, utc_now()),
        )
    elif status == "completed":
        execute_db(
            "UPDATE scheduled_jobs SET status=%s, completed_at=%s, updated_at=%s WHERE id=%s",
            (status, utc_now(), utc_now(), job_id),
        )
    else:
        execute_db(
            "UPDATE scheduled_jobs SET status=%s, locked_at=%s, updated_at=%s WHERE id=%s",
            (status, utc_now(), utc_now(), job_id),
        )
