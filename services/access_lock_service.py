"""Lock and unlock a location's access to the system based on billing.

The only writer of locations.access_locked. Called from
services/automatic_billing_service.py (the only place that actually knows
whether a bill went unpaid) and from routes/billing_wall.py (when a
locked-out owner successfully pays from the wall itself, for an
immediate unlock rather than waiting for the next cron cycle).
"""
from __future__ import annotations

from database import execute_db, query_db, utc_now


def lock_location(location_id: int, reason: str) -> bool:
    """Returns whether a row was actually affected -- RETURNING id, same
    pattern services/automatic_billing_service.py's _claim_billing_record()
    uses. Neither this function nor unlock_location() has an ownership
    check of its own to add: unlike _claim_billing_record (which has
    billing_id to cross-check against location_id), these operate on
    locations.id alone, with no second identifier to validate against --
    checked directly, tests/unit/test_access_lock_tenant_isolation.py
    proves every live caller's location_id is independently trustworthy
    before it ever reaches here, which is the actual security boundary.
    What was missing was visibility: a nonexistent location_id previously
    updated zero rows and returned None either way, indistinguishable
    from success. Existing callers that ignore the return value are
    unaffected -- this is additive."""
    result = query_db(
        "UPDATE locations SET access_locked=TRUE, access_locked_reason=%s, access_locked_at=%s, updated_at=%s "
        "WHERE id=%s RETURNING id",
        (reason, utc_now(), utc_now(), location_id),
    )
    return bool(result)


def unlock_location(location_id: int) -> bool:
    result = query_db(
        "UPDATE locations SET access_locked=FALSE, access_locked_reason=NULL, access_locked_at=NULL, updated_at=%s "
        "WHERE id=%s RETURNING id",
        (utc_now(), location_id),
    )
    return bool(result)
