"""Classifies a Flyer Lady publish failure as retryable or permanent,
and computes the resulting SpecialPost state.

Kept separate from publish_service.py deliberately: this is pure
classification logic with no external API calls of its own, easiest to
test in isolation, and the smallest possible change to
FlyerLadyPublishService.publish_post() is to have its except block call
straight into this.

Classification is based on the signals MetaGraphAPIError and
GoogleBusinessAPIError already carry (status_code, error) -- both
follow the identical shape, and the retryable/permanent split by status
code below matches the one already established in
integrations/meta/services/tech_provider_onboarding_service.py's own
_graph_error() (status is None or >=500 or ==429 -> retryable), so this
isn't a new convention, just the same one applied here.

Deliberately conservative for anything NOT explicitly recognized:
defaults to retryable. Before this change, every single failure
retried (with backoff, forever); silently reclassifying an unfamiliar
error shape as permanent would be a real behavior regression for
whatever that error turns out to be, not just a missed optimization.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

#: A post that has failed this many times will not be retried again,
#: regardless of whether the failure itself was retryable.
MAX_ATTEMPTS = 5

#: Distinct from a post's default "pending" or the retryable "failed" --
#: the scheduler's own claim query (jobs/flyer_lady.py, status IN
#: (pending, failed)) does not select this status, which is what
#: actually stops the retries. Setting next_attempt_at to None alone is
#: not enough for that: the same query treats a NULL next_attempt_at as
#: eligible immediately, not never.
STATUS_FAILED_PERMANENTLY = "failed_permanently"

#: The existing status a retryable failure already used before this
#: change -- kept as-is so an in-progress retry cycle looks the same.
STATUS_FAILED_RETRYABLE = "failed"

#: Reused verbatim from the existing WhatsApp connection status
#: vocabulary (integrations/meta/services/token_status_service.py) --
#: not a new value invented for Flyer Lady specifically.
RECONNECT_REQUIRED = "reconnect_required"


@dataclass(frozen=True)
class ErrorClassification:
    retryable: bool
    is_auth_error: bool = False


def classify_error(exc: Exception) -> ErrorClassification:
    """RETRYABLE: network errors (no status_code at all), HTTP 429,
    temporary 5xx.
    PERMANENT: invalid/revoked token or missing permission (401/403 --
    both mean "reconnect_required" below, since this codebase doesn't
    reliably distinguish an expired token from a scope the connection
    never had at the HTTP layer alone), unsupported platform capability
    and invalid configuration (both surface as a plain ValueError raised
    directly by publish_post() itself, before any external call is
    attempted -- "unsupported platform: X", "... is not connected",
    "special not found", "special has not been approved"), and any
    other non-2xx response publish_post() did not explicitly check for
    (a genuine bad-request-shaped rejection that will not succeed
    unmodified on retry).
    """
    status_code = getattr(exc, "status_code", None)

    if isinstance(exc, ValueError) and status_code is None:
        return ErrorClassification(retryable=False)

    if status_code is not None:
        if status_code == 429 or status_code >= 500:
            return ErrorClassification(retryable=True)
        if status_code in (401, 403):
            return ErrorClassification(retryable=False, is_auth_error=True)
        return ErrorClassification(retryable=False)

    return ErrorClassification(retryable=True)


def apply_failure(post, exc: Exception) -> ErrorClassification:
    """Mutates `post` to reflect this failed attempt. Does not touch the
    session or commit -- publish_post() already owns that, and already
    incremented post.attempts before the external call was attempted,
    so this reads that count rather than tracking its own.
    """
    classification = classify_error(exc)
    post.error_message = str(exc)[:4000]

    if not classification.retryable or post.attempts >= MAX_ATTEMPTS:
        post.status = STATUS_FAILED_PERMANENTLY
        post.next_attempt_at = None
    else:
        post.status = STATUS_FAILED_RETRYABLE
        post.next_attempt_at = datetime.now(timezone.utc) + timedelta(
            minutes=min(60, 2 ** min(post.attempts, 5))
        )

    return classification
