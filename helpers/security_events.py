"""PHANTA's own security event log.

WHY THIS IS SEPARATE FROM audit_logs
------------------------------------
audit_logs records what happened *inside a workshop* -- it is tenant data,
scoped by location_id, and protected by row level security keyed on
app.location_id.

Authentication events are a different class of information:

  * A failed login has no tenant at all. The submitted address may not match
    any account, so there is no location to attribute the event to.
  * Successful logins, password changes, resets and deactivations are
    PHANTA's own operational security records, not workshop customer data
    that PHANTA processes on behalf of a workshop.
  * The two will not share a retention period.

Writing them into audit_logs would have required loosening that table's RLS to
tolerate NULL location_id, which would have let every tenant read every
NULL-location row -- including other tenants' login failures. This table
avoids that: it is append-only for the application and readable only in the
platform-admin context.

NEVER STORE THE RAW SUBMITTED IDENTIFIER
----------------------------------------
People routinely type their password into the username field. A column that
records whatever was submitted on a failed login therefore accumulates real
passwords in plaintext -- the same class of problem as the legacy plaintext
password column that was removed from services/auth_service.py.

So: when the submitted identifier matches a known account, the account's own
address is stored (PHANTA already holds it). When it does not, only a keyed
hash is stored. The hash still allows "how many attempts came from the same
made-up identifier" without ever persisting the value itself.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os

from database import execute_db, utc_now

logger = logging.getLogger(__name__)

# Event types. Kept as constants so call sites cannot drift.
LOGIN_SUCCEEDED = "auth.login_succeeded"
LOGIN_FAILED = "auth.login_failed"
LOGOUT = "auth.logout"
PASSWORD_CHANGED = "auth.password_changed"
PASSWORD_RESET_BY_ADMIN = "auth.password_reset_by_admin"
ACCOUNT_DEACTIVATED = "auth.account_deactivated"
ACCOUNT_REACTIVATED = "auth.account_reactivated"
SESSION_REVOKED = "auth.session_revoked"


def hash_identifier(value: str) -> str:
    """Return a keyed hash of a submitted identifier.

    Keyed with FLASK_SECRET_KEY so the digests are not comparable against a
    precomputed rainbow table of common email addresses. Truncated because
    only correlation is needed, not reversibility.
    """
    key = (os.getenv("FLASK_SECRET_KEY") or os.getenv("DEV_FLASK_SECRET_KEY") or "phanta").encode()
    digest = hmac.new(key, (value or "").strip().lower().encode(), hashlib.sha256).hexdigest()
    return digest[:32]


def _client_ip() -> str | None:
    """Best-effort client address. Accurate only because ProxyFix is applied."""
    try:
        from flask import has_request_context, request

        if not has_request_context():
            return None
        return request.remote_addr
    except Exception:
        return None


def record_security_event(
    event_type: str,
    *,
    user_id=None,
    identifier: str | None = None,
    identifier_is_known_account: bool = False,
    location_id=None,
    outcome: str = "success",
    details: dict | None = None,
) -> None:
    """Append one security event.

    Deliberately never raises: a failure to write the security log must not
    prevent a user from logging in or changing their password. Failures are
    logged so they are still visible in Sentry.
    """
    try:
        stored_identifier = None
        identifier_hash = None
        if identifier:
            if identifier_is_known_account:
                stored_identifier = identifier.strip().lower()[:255]
            else:
                identifier_hash = hash_identifier(identifier)

        execute_db(
            """
            INSERT INTO security_events (
                event_type, outcome, user_id, location_id,
                identifier, identifier_hash, ip_address, details_json, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event_type,
                outcome,
                user_id,
                location_id,
                stored_identifier,
                identifier_hash,
                _client_ip(),
                json.dumps(details or {}, separators=(",", ":"), sort_keys=True),
                utc_now(),
            ),
        )
    except Exception:
        logger.exception("security_event_write_failed event_type=%s", event_type)


def fetch_security_events(limit: int = 200, event_type: str | None = None):
    """Read recent security events.

    Only usable in the platform-admin context: under PostgreSQL the table's
    RLS grants SELECT solely when app.platform_admin is set.
    """
    from database import query_db

    clauses = []
    args = []
    if event_type:
        clauses.append("event_type=%s")
        args.append(event_type)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    args.append(limit)
    return query_db(
        f"""
        SELECT * FROM security_events
        {where}
        ORDER BY created_at DESC
        LIMIT %s
        """,
        tuple(args),
    ) or []
