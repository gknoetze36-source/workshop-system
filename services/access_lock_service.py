"""Lock and unlock a location's access to the system based on billing.

The only writer of locations.access_locked. Called from
services/automatic_billing_service.py (the only place that actually knows
whether a bill went unpaid) and from routes/billing_wall.py (when a
locked-out owner successfully pays from the wall itself, for an
immediate unlock rather than waiting for the next cron cycle).
"""
from __future__ import annotations

from database import execute_db, utc_now


def lock_location(location_id: int, reason: str):
    execute_db(
        "UPDATE locations SET access_locked=TRUE, access_locked_reason=%s, access_locked_at=%s, updated_at=%s WHERE id=%s",
        (reason, utc_now(), utc_now(), location_id),
    )


def unlock_location(location_id: int):
    execute_db(
        "UPDATE locations SET access_locked=FALSE, access_locked_reason=NULL, access_locked_at=NULL, updated_at=%s WHERE id=%s",
        (utc_now(), location_id),
    )
