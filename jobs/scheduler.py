"""PHANTA scheduled-job entry point.

This module is the short-lived command executed by the dedicated Railway Cron
service. Every job must finish and return so Railway can mark the execution
complete and schedule the next run.
"""
from __future__ import annotations

import logging

from observability import configure_logging, init_sentry, capture_exception

configure_logging()
init_sentry()

from .meta_token_monitor import run_meta_token_monitor
from .lifecycle_communication import run_lifecycle_communication
from .follow_up import run_follow_up_worker
from .flyer_lady import run_flyer_lady_publish_queue
from .paystack_reconciliation import run_paystack_reconciliation
from services.automation_engine import process_due_automation_jobs

logger = logging.getLogger(__name__)


def _run(name, func):
    """Run one scheduled job without allowing one failure to cancel the rest."""
    try:
        result = func()
        logger.info("scheduled job completed: %s", name)
        return {"status": "ok", "result": result}
    except Exception as exc:
        logger.exception("scheduled job failed: %s", name)
        capture_exception(exc)
        return {"status": "error", "error": str(exc)}


def run_scheduled_jobs() -> dict:
    """Execute all production scheduled jobs once, then return."""
    return {
        "meta_token_monitor": _run("meta_token_monitor", run_meta_token_monitor),
        "lifecycle_communication": _run("lifecycle_communication", run_lifecycle_communication),
        "follow_up": _run("follow_up", run_follow_up_worker),
        "flyer_lady": _run("flyer_lady", run_flyer_lady_publish_queue),
        "paystack_reconciliation": _run("paystack_reconciliation", run_paystack_reconciliation),
        "automation_engine": _run("automation_engine", process_due_automation_jobs),
    }


def run_billing_jobs() -> dict:
    """Execute the billing cycle once, then return.

    Deliberately NOT part of run_scheduled_jobs(). That runs every 5 minutes
    (railway-cron.toml), which is the right cadence for message queues and
    token checks but wrong for charging customers' cards -- billing needs
    its own, much slower schedule. This is the entry point for the separate
    Railway cron service configured by railway-cron-billing.toml.
    """
    from .billing import run_billing_cycle
    return {"billing": _run("billing", run_billing_cycle)}


if __name__ == "__main__":
    import json
    print(json.dumps(run_scheduled_jobs(), default=str))
