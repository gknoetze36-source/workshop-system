"""Data retention rules and their execution.

DESIGN
------
Retention is expressed as a list of rules rather than a single hard-coded
query, so adding a rule later is a data change rather than a new job. Each
rule states: what data, how long, and whether expiry means deletion or
anonymisation.

RETENTION PERIODS ARE POLICY, NOT CODE
--------------------------------------
Only ONE rule is defined here, because only one period has actually been
decided:

  * Raw inbound/outbound WhatsApp message text: 14 days.

Everything else -- billing records, invoices, audit logs, security events,
booking history -- REQUIRES LEGAL/ACCOUNTING CONFIRMATION of the applicable
South African retention period before a rule is written. Inventing a period
would be worse than having none: it would look authoritative while being
arbitrary, and it would silently destroy records the business may be
required to keep.

Add rules here only once the period is confirmed and recorded.

WHY 14 DAYS FOR MESSAGE TEXT
----------------------------
The Service Advisor needs recent conversation turns to hold a coherent
exchange -- "yes, Tuesday works" is meaningless without the question before
it. A workshop job (enquiry, booking, reminder, ready, collected) completes
well inside two weeks, after which the raw text has no operational value.

What survives is the structured record: the message row itself (direction,
channel, status, timestamps) and everything extracted into bookings,
vehicles and conversation summaries. Only the free text is cleared, because
free text is where a customer may have mentioned anything at all, including
information PHANTA never asked for and does not want to hold.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from database import query_db, execute_db, utc_now

logger = logging.getLogger(__name__)

# Confirmed retention period. Overridable for testing, not for production
# convenience -- changing it changes what PHANTA's privacy documentation says.
MESSAGE_BODY_RETENTION_DAYS = int(os.getenv("MESSAGE_BODY_RETENTION_DAYS", "14"))

# Placeholder written in place of cleared text, so the row remains readable as
# "this message existed and its content has expired" rather than looking like
# an empty or failed message.
CLEARED_PLACEHOLDER = "[content removed: retention period expired]"


def _cutoff(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).replace(microsecond=0).isoformat()


def clear_expired_message_bodies(days: int | None = None, dry_run: bool = False) -> dict:
    """Clear raw message text older than the retention period.

    Anonymisation, not deletion: the message row is kept so conversation
    structure, delivery status and volume history remain intact. Only the
    body text is removed.

    Returns a summary dict. Safe to run repeatedly -- rows already cleared are
    excluded, so a second run in the same day does nothing.
    """
    days = MESSAGE_BODY_RETENTION_DAYS if days is None else days
    cutoff = _cutoff(days)

    pending = query_db(
        """
        SELECT COUNT(*) AS c
        FROM messages
        WHERE created_at < %s
          AND body IS NOT NULL
          AND body <> %s
        """,
        (cutoff, CLEARED_PLACEHOLDER),
        one=True,
    )
    count = int((pending or {}).get("c") or 0)

    if dry_run or not count:
        return {"rule": "message_body", "days": days, "cutoff": cutoff,
                "cleared": 0, "pending": count, "dry_run": dry_run}

    execute_db(
        """
        UPDATE messages
        SET body = %s
        WHERE created_at < %s
          AND body IS NOT NULL
          AND body <> %s
        """,
        (CLEARED_PLACEHOLDER, cutoff, CLEARED_PLACEHOLDER),
    )

    logger.info(
        "retention_message_bodies_cleared count=%s cutoff=%s days=%s",
        count, cutoff, days,
    )
    return {"rule": "message_body", "days": days, "cutoff": cutoff,
            "cleared": count, "pending": 0, "dry_run": False}


# Rules executed by the scheduled retention job, in order.
RETENTION_RULES = [
    clear_expired_message_bodies,
]

# Data types whose retention period is NOT yet decided. Listed explicitly so
# the gap is visible in code rather than being an unnoticed omission.
AWAITING_LEGAL_CONFIRMATION = (
    "billing_records",
    "invoices",
    "payment_transactions",
    "audit_logs",
    "security_events",
    "booking_history",
    "communication_logs",
)


def run_retention(dry_run: bool = False) -> list[dict]:
    """Execute every defined retention rule. Called by the scheduler."""
    results = []
    for rule in RETENTION_RULES:
        try:
            results.append(rule(dry_run=dry_run))
        except Exception:
            logger.exception("retention_rule_failed rule=%s", getattr(rule, "__name__", rule))
            results.append({"rule": getattr(rule, "__name__", str(rule)), "error": True})
    return results
