"""Workshop (tenant) offboarding.

THE CHAIN
---------
    ACCOUNT TERMINATION
      -> ACCESS DISABLED
      -> INTEGRATIONS DISCONNECTED
      -> DATA EXPORT OPPORTUNITY
      -> DATA RETENTION REVIEW
      -> DATA DELETION
      -> BACKUP LIFECYCLE
      -> AUDIT RECORD

DELIBERATELY TWO STAGES, NOT ONE
--------------------------------
Offboarding is split into begin_offboarding() and complete_offboarding()
because the middle of that chain is not instantaneous and must not be:

  * the workshop needs a real opportunity to export its data before it is
    destroyed -- a one-click "terminate and purge" removes that opportunity
  * some records must survive termination (see below), and deciding which
    requires the retention review to have happened

begin_offboarding() stops the account immediately: access off, integrations
disconnected, no further messages sent. That is the urgent part and it is
reversible. complete_offboarding() destroys data and is not reversible, so it
is a separate, later, deliberate act.

WHAT SURVIVES TERMINATION, AND WHY
----------------------------------
Deleting a tenant is not "delete every row with this location_id":

  * Billing and invoice records support tax and company-law obligations. The
    applicable South African retention period is NOT decided (see
    services/retention_service.AWAITING_LEGAL_CONFIRMATION), so these are
    retained and flagged for review rather than destroyed on a guess.
  * Legal acceptance records evidence what was agreed. Destroying them on
    termination would destroy the evidence that the terms were ever accepted.
  * Audit and security logs are the accountability record, including the
    record of this very offboarding.

Customer personal information IS anonymised, using the same
DataLifecycleService path as an individual erasure request, so the two
behave identically.

BACKUPS
-------
This module cannot reach into Railway's backups, and pretending otherwise
would be dishonest. It records the date after which the tenant's data will
have aged out of the backup rotation, so the question "is it really gone?"
has a documented answer rather than an assumption.
"""
from __future__ import annotations

import logging
import os

from database import query_db, execute_db, utc_now, get_session

logger = logging.getLogger(__name__)

# Backup retention window of the hosting provider. NOT a PHANTA setting --
# it must match what Railway/PostgreSQL actually retains. Verify before
# relying on the date this produces.
BACKUP_RETENTION_DAYS = int(os.getenv("BACKUP_RETENTION_DAYS", "30"))

STAGE_ACTIVE = "active"
STAGE_TERMINATING = "terminating"
STAGE_COMPLETED = "completed"


def begin_offboarding(location_id: int, actor: str, reason: str | None = None) -> dict:
    """Stage 1: stop the account. Reversible.

    Disables access and disconnects integrations so nothing further is sent
    or received, while leaving the data intact so it can still be exported.
    """
    from services.access_lock_service import lock_location

    location = query_db("SELECT id, name FROM locations WHERE id=%s", (location_id,), one=True)
    if not location:
        raise LookupError("location not found")

    # ACCESS DISABLED -- reuses the existing billing access-lock mechanism,
    # which auth_service already enforces at login.
    lock_location(location_id, reason or "account terminated")

    # Every user of this location loses their session immediately rather than
    # continuing until their cookie happens to expire.
    users = query_db("SELECT id FROM users WHERE location_id=%s", (location_id,)) or []
    from services.auth_service import bump_session_version
    for user in users:
        bump_session_version(user["id"])
    execute_db(
        "UPDATE users SET active=%s, updated_at=%s WHERE location_id=%s",
        (False, utc_now(), location_id),
    )

    # INTEGRATIONS DISCONNECTED -- marking the connections revoked stops the
    # messaging and publishing paths, which resolve connections by status.
    disconnected = _disconnect_integrations(location_id)

    _record(location_id, actor, "tenant.offboarding_started", {
        "reason": reason,
        "users_deactivated": len(users),
        "integrations_disconnected": disconnected,
    })

    return {
        "stage": STAGE_TERMINATING,
        "location_id": location_id,
        "users_deactivated": len(users),
        "integrations_disconnected": disconnected,
        "next_step": (
            "Give the workshop its data export, then run the retention review "
            "before calling complete_offboarding()."
        ),
    }


def _disconnect_integrations(location_id: int) -> dict:
    """Mark external connections revoked. Returns what was touched."""
    results = {}
    for table in ("meta_business_connections", "meta_social_connections", "google_business_connections"):
        try:
            execute_db(
                f"UPDATE {table} SET connection_status=%s WHERE location_id=%s",
                ("revoked", location_id),
            )
            row = query_db(
                f"SELECT COUNT(*) AS c FROM {table} WHERE location_id=%s",
                (location_id,), one=True,
            )
            results[table] = int((row or {}).get("c") or 0)
        except Exception:
            logger.exception("integration_disconnect_failed table=%s location_id=%s", table, location_id)
            results[table] = "error"
    return results


def offboarding_readiness(location_id: int) -> dict:
    """What still needs doing before deletion. Read-only."""
    location = query_db(
        "SELECT id, name, active, access_locked FROM locations WHERE id=%s",
        (location_id,), one=True,
    )
    if not location:
        raise LookupError("location not found")

    customers = query_db(
        "SELECT COUNT(*) AS c FROM customers WHERE location_id=%s AND deleted_at IS NULL",
        (location_id,), one=True,
    )
    active_users = query_db(
        "SELECT COUNT(*) AS c FROM users WHERE location_id=%s AND active=TRUE",
        (location_id,), one=True,
    )
    exported = query_db(
        "SELECT COUNT(*) AS c FROM audit_logs WHERE location_id=%s AND action=%s",
        (location_id, "data.exported"), one=True,
    )

    return {
        "location_id": location_id,
        "access_locked": bool(location.get("access_locked")),
        "active_users_remaining": int((active_users or {}).get("c") or 0),
        "customers_pending_anonymisation": int((customers or {}).get("c") or 0),
        "data_export_taken": int((exported or {}).get("c") or 0) > 0,
        "retention_review_required": [
            "billing_records", "invoices", "payment_transactions",
        ],
    }


def complete_offboarding(location_id: int, actor: str, force: bool = False) -> dict:
    """Stage 2: anonymise customer personal information. NOT reversible.

    Refuses to run unless the workshop has had a data export, because the
    export opportunity is a step in the chain rather than a courtesy. `force`
    exists for the case where the workshop explicitly declines an export --
    that decision is then recorded in the audit trail.
    """
    readiness = offboarding_readiness(location_id)
    if not readiness["data_export_taken"] and not force:
        raise RuntimeError(
            "no data export recorded for this location; provide the export "
            "opportunity first, or pass force=True to record that it was declined"
        )

    from repositories.audit_repo import AuditLogRepository
    from services.data_lifecycle import DataLifecycleService

    customers = query_db(
        "SELECT id FROM customers WHERE location_id=%s AND deleted_at IS NULL",
        (location_id,),
    ) or []

    anonymised, failed = 0, 0
    session = get_session()
    try:
        service = DataLifecycleService(session, AuditLogRepository(session))
        for customer in customers:
            try:
                service.soft_delete_customer(location_id, customer["id"], actor)
                anonymised += 1
            except Exception:
                logger.exception(
                    "offboarding_customer_anonymise_failed customer_id=%s", customer["id"]
                )
                failed += 1
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    execute_db(
        "UPDATE locations SET active=%s, updated_at=%s WHERE id=%s",
        (False, utc_now(), location_id),
    )

    from datetime import datetime, timedelta, timezone
    backup_clear_date = (
        datetime.now(timezone.utc) + timedelta(days=BACKUP_RETENTION_DAYS)
    ).date().isoformat()

    result = {
        "stage": STAGE_COMPLETED,
        "location_id": location_id,
        "customers_anonymised": anonymised,
        "customers_failed": failed,
        "export_was_taken": readiness["data_export_taken"],
        "forced": force and not readiness["data_export_taken"],
        "retained_for_legal_obligation": [
            "invoices", "payment_transactions", "legal_acceptances",
            "audit_logs", "security_events",
        ],
        "backup_clear_date": backup_clear_date,
        "backup_note": (
            f"Anonymised data may persist in hosting-provider backups until "
            f"approximately {backup_clear_date} ({BACKUP_RETENTION_DAYS}-day "
            f"rotation). Verify this window against the provider's actual "
            f"configuration before relying on it."
        ),
    }

    _record(location_id, actor, "tenant.offboarding_completed", result)
    logger.warning(
        "tenant_offboarding_completed location_id=%s anonymised=%s failed=%s",
        location_id, anonymised, failed,
    )
    return result


def _record(location_id, actor, action, details):
    """Write the offboarding step to both the tenant audit log and the
    platform security log. Both, because the tenant's own audit trail should
    show what happened to their account, and PHANTA needs a platform-level
    record that survives the tenant being deactivated.

    The audit_logs write is wrapped in the tenant's own RLS scope. Offboarding
    is performed by a platform administrator, and audit_logs' platform policy
    is SELECT-only -- so without this, the INSERT is rejected by PostgreSQL
    with "new row violates row-level security policy for table audit_logs" and
    the whole offboarding fails. The audit row belongs to that tenant, so
    writing it in that tenant's scope is also the semantically correct thing to
    do rather than a workaround.

    security_events has an append-only INSERT policy and needs no scope.
    """
    from database.query import raw_location_scope
    from helpers.audit import record_audit
    from helpers.security_events import record_security_event

    if location_id:
        with raw_location_scope(int(location_id)):
            record_audit(
                action, "location", entity_id=location_id,
                location_id=location_id, details=details,
            )
    else:
        record_audit(action, "location", entity_id=location_id, details=details)

    record_security_event(
        f"privacy.{action.split('.')[-1]}",
        location_id=location_id,
        details={"actor": actor, **{k: v for k, v in details.items() if isinstance(v, (str, int, bool))}},
    )
