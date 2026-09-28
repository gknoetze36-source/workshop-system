"""Enforces the two billing limits X posting requires: VANTA pays for X
API usage directly, unlike Facebook/Instagram/Google Business Profile,
which cost VANTA nothing beyond hosting. This module is what makes
"never allow a retry bug to create unlimited X API charges" true.

The two limits, from XUsageCounter (models/integration_models.py):
  - a per-tenant monthly post limit (X_PER_TENANT_MONTHLY_POST_LIMIT)
  - a global, VANTA-wide monthly post limit across every tenant
    combined (X_GLOBAL_MONTHLY_POST_LIMIT)

Both numbers are modeled as a POST COUNT, not a dollar figure. X's own
pricing (docs.x.com/x-api/x-api-v2/tweets/tweets-lookup) is a flat
monthly subscription per tier with a fixed posts-per-month cap (Free:
500 posts/month; Basic: $200/month for up to 10,000 posts/month) --
there is no true per-call variable cost to track in dollars, and this
codebase has no live X billing data to check a dollar figure against
even if there were. A post-count cap is the thing that is both real
and actually enforceable from here.

The load-bearing guarantee: check_and_reserve_x_post() performs one
single atomic `UPDATE ... SET count = count + 1 WHERE count < :limit`
per limit, not a separate read-then-write. A read-then-write (SELECT
count, compare in Python, then UPDATE) leaves a real race window under
concurrent workers -- two requests could both read the same
under-the-limit count and both proceed. The atomic UPDATE's WHERE
clause makes the database itself the single point of truth: it can
only ever succeed count_at_call_time < limit times in a month,
regardless of how many callers race for it, and a bug that retries this
same call many times in a tight loop still only ever succeeds up to the
limit -- every call past that returns False and raises, spending
nothing further.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import text

#: Deliberately conservative starting values -- comfortably inside the
#: cheapest paid X API tier's own posts-per-month cap (Basic: 10,000/mo
#: as of the pricing referenced above), leaving real headroom before
#: VANTA's own X bill could be affected by Flyer Lady's usage at all.
#: Not derived from any specific tenant's contract; a deliberate,
#: reviewable default.
X_PER_TENANT_MONTHLY_POST_LIMIT = 30
X_GLOBAL_MONTHLY_POST_LIMIT = 3000

_GLOBAL_SCOPE_KEY = "global"


class XSpendLimitExceeded(ValueError):
    """A plain ValueError subclass -- retry_policy.py's existing
    classifier (flyer_lady/retry_policy.py) already treats a bare
    ValueError with no status_code as a permanent, non-retryable
    error, exactly the behavior wanted here, with zero changes needed
    to that classifier. Subclassed only so callers/tests can catch it
    specifically without string-matching a message."""


def _current_month_key(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return now.strftime("%Y-%m")


def _try_reserve(session, *, scope: str, scope_key: str, month_key: str, limit: int) -> bool:
    """Atomically increments the counter for this scope, only if doing
    so would not exceed limit. Returns whether it succeeded."""
    from models.integration_models import XUsageCounter

    existing = session.query(XUsageCounter).filter_by(scope=scope, scope_key=scope_key, month_key=month_key).one_or_none()
    if existing is None:
        # Row-creation race is intentionally not itself atomic (SQLite
        # has no upsert this codebase already uses elsewhere, and the
        # unique constraint on (scope, scope_key, month_key) makes a
        # duplicate-row race merely raise an IntegrityError rather than
        # double count) -- the atomic UPDATE just below is what
        # actually enforces the limit; this only ensures a row exists
        # for it to act on.
        try:
            session.add(XUsageCounter(scope=scope, scope_key=scope_key, month_key=month_key, count=0))
            session.flush()
        except Exception:
            session.rollback()

    result = session.execute(
        text(
            "UPDATE x_usage_counters SET count = count + 1 "
            "WHERE scope = :scope AND scope_key = :scope_key AND month_key = :month_key AND count < :limit"
        ),
        {"scope": scope, "scope_key": scope_key, "month_key": month_key, "limit": limit},
    )
    return result.rowcount == 1


def check_and_reserve_x_post(session, location_id: int, *, now: datetime | None = None) -> None:
    """Must be called, and must succeed, immediately before any X API
    call that could result in a real, billable post -- see
    flyer_lady/platforms/x_publisher.py, which calls this as its very
    first line, before the access token is even read. Raises
    XSpendLimitExceeded (never returns False) so a caller cannot
    accidentally ignore the result and proceed anyway.

    Reserves the tenant slot first, then the global slot. If the global
    reservation fails after the tenant one already succeeded, the
    tenant counter is left incremented -- deliberately: this call site
    is about to be blocked from posting either way, and giving back the
    tenant slot would let a location's own limit be checked again this
    same month once global capacity frees up, silently changing what
    "per-tenant limit" means from a stated cap into a moving target.
    """
    now = now or datetime.now(timezone.utc)
    month_key = _current_month_key(now)

    if not _try_reserve(session, scope="tenant", scope_key=str(location_id), month_key=month_key, limit=X_PER_TENANT_MONTHLY_POST_LIMIT):
        raise XSpendLimitExceeded(
            f"Monthly X post limit reached for this location ({X_PER_TENANT_MONTHLY_POST_LIMIT} posts/month)"
        )

    if not _try_reserve(session, scope="global", scope_key=_GLOBAL_SCOPE_KEY, month_key=month_key, limit=X_GLOBAL_MONTHLY_POST_LIMIT):
        raise XSpendLimitExceeded(
            f"VANTA's global monthly X posting limit has been reached ({X_GLOBAL_MONTHLY_POST_LIMIT} posts/month)"
        )
