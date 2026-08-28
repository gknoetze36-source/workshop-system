"""Security incident register.

WHY THIS EXISTS
---------------
A breach response plan that exists only as a document is not a capability.
When something happens, the questions that have to be answered quickly are:

  * what happened, and when was it detected
  * which tenant and which system are affected
  * WHICH RECORDS may have been exposed
  * what was contained, and when
  * who was notified, and when

This module provides the record. It does not replace judgement about whether
a given incident is notifiable -- that is a legal decision -- but it makes the
factual basis for that decision retrievable instead of scattered across
memory, Slack and Sentry.

RELATIONSHIP TO THE OTHER LOGS
------------------------------
  * security_events (0025) is the automatic, append-only stream of individual
    auth/account events. High volume, machine-written.
  * audit_logs is per-tenant business activity.
  * incidents (here) are human-declared: one row per investigation, updated as
    it progresses, linking to whatever evidence the other two hold.

SCOPING A BREACH
----------------
identify_affected_customers() answers "whose data was in this?" for the two
cases PHANTA can actually determine from its own records: a whole location, or
a specific list of customers. It deliberately does not guess. If the scope
cannot be determined from the data, the incident records that fact rather than
implying a false precision.
"""
from __future__ import annotations

import json
import logging

from database import query_db, execute_db, utc_now

logger = logging.getLogger(__name__)

# Lifecycle.
STATUS_OPEN = "open"
STATUS_INVESTIGATING = "investigating"
STATUS_CONTAINED = "contained"
STATUS_RESOLVED = "resolved"

# Severity. Deliberately few: more options mean less consistent grading.
SEVERITY_LOW = "low"
SEVERITY_MEDIUM = "medium"
SEVERITY_HIGH = "high"
SEVERITY_CRITICAL = "critical"

INCIDENT_TYPES = (
    "unauthorised_access",
    "data_disclosure",
    "credential_compromise",
    "integration_compromise",
    "availability",
    "misdirected_communication",
    "other",
)


def open_incident(
    *,
    incident_type: str,
    severity: str,
    summary: str,
    detected_by: str,
    location_id=None,
    system_affected: str | None = None,
    data_categories: list | None = None,
    detected_at: str | None = None,
) -> int:
    """Record a newly detected incident. Returns the incident id.

    `location_id` may be None: an incident affecting PHANTA's own
    infrastructure is not scoped to one tenant, and forcing a tenant onto it
    would misrepresent the blast radius.
    """
    execute_db(
        """
        INSERT INTO security_incidents (
            incident_type, severity, status, summary, detected_by, detected_at,
            location_id, system_affected, data_categories_json,
            created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            incident_type, severity, STATUS_OPEN, summary, detected_by,
            detected_at or utc_now(), location_id, system_affected,
            json.dumps(data_categories or [], separators=(",", ":")),
            utc_now(), utc_now(),
        ),
    )
    row = query_db("SELECT MAX(id) AS id FROM security_incidents", one=True)
    incident_id = int((row or {}).get("id") or 0)
    logger.warning(
        "security_incident_opened id=%s type=%s severity=%s location_id=%s",
        incident_id, incident_type, severity, location_id,
    )
    return incident_id


def update_incident(incident_id: int, **fields) -> bool:
    """Update an incident's investigation state.

    Only the fields an investigation actually revises are writable. The
    detection facts (type, detected_at, detected_by) are not: rewriting how an
    incident was found undermines the record's value as evidence.
    """
    allowed = {
        "status", "severity", "summary", "system_affected",
        "containment_actions", "investigation_notes", "recovery_actions",
        "affected_record_count", "notifications_sent", "resolved_at",
    }
    invalid = set(fields) - allowed
    if invalid:
        raise ValueError(f"unsupported incident fields: {sorted(invalid)}")
    if not fields:
        return False

    sets = ", ".join(f"{key}=%s" for key in fields)
    execute_db(
        f"UPDATE security_incidents SET {sets}, updated_at=%s WHERE id=%s",
        (*fields.values(), utc_now(), incident_id),
    )
    return True


def get_incident(incident_id: int):
    return query_db("SELECT * FROM security_incidents WHERE id=%s", (incident_id,), one=True)


def list_incidents(status: str | None = None, limit: int = 100):
    clauses, args = [], []
    if status:
        clauses.append("status=%s")
        args.append(status)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    args.append(limit)
    return query_db(
        f"SELECT * FROM security_incidents {where} ORDER BY detected_at DESC LIMIT %s",
        tuple(args),
    ) or []


def identify_affected_customers(location_id=None, customer_ids=None):
    """Determine whose personal information an incident may have involved.

    Returns the customer records in scope, so a notification decision rests on
    an actual list rather than an estimate. Two determinable cases:

      * location_id: everything belonging to one workshop
      * customer_ids: a specific known set

    Returns an empty list when neither is supplied -- "we cannot determine the
    scope" is a real and important answer, and inventing one would be worse
    than admitting it.
    """
    if customer_ids:
        placeholders = ", ".join(["%s"] * len(customer_ids))
        return query_db(
            f"""
            SELECT id, location_id, first_name, last_name, whatsapp_number, email
            FROM customers WHERE id IN ({placeholders})
            """,
            tuple(customer_ids),
        ) or []

    if location_id:
        return query_db(
            """
            SELECT id, location_id, first_name, last_name, whatsapp_number, email
            FROM customers WHERE location_id=%s AND deleted_at IS NULL
            """,
            (location_id,),
        ) or []

    return []


def scope_incident(incident_id: int, location_id=None, customer_ids=None) -> dict:
    """Attach an affected-record count to an incident.

    Records the COUNT on the incident, not the customer list itself. Copying
    the affected people's names and numbers into the incident record would
    create a second store of the very data the incident is about.
    """
    affected = identify_affected_customers(location_id=location_id, customer_ids=customer_ids)
    update_incident(incident_id, affected_record_count=len(affected))
    return {
        "incident_id": incident_id,
        "affected_customers": len(affected),
        "scope_determined": bool(location_id or customer_ids),
    }
