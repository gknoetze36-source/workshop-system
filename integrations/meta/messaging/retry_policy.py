"""Deterministic outbound retry policy for Meta messaging."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetryDecision:
    retryable: bool
    delay_seconds: int
    reason: str


class MetaRetryPolicy:
    """Classify failures without retrying permanent customer/data errors."""

    MAX_ATTEMPTS = 3
    BACKOFF_SECONDS = (2, 8, 30)

    RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504}
    # Meta error codes can vary by endpoint/version. HTTP status remains the
    # first signal; known permanent validation/permission errors are excluded.
    PERMANENT_ERROR_CODES = {"100", "200", "131026", "132001", "132015", "132016"}

    def decide(self, *, attempt_number: int, http_status: int | None = None, meta_error_code: str | None = None) -> RetryDecision:
        if attempt_number >= self.MAX_ATTEMPTS:
            return RetryDecision(False, 0, "max_attempts_reached")
        if meta_error_code and str(meta_error_code) in self.PERMANENT_ERROR_CODES:
            return RetryDecision(False, 0, "permanent_meta_error")
        if http_status in self.RETRYABLE_HTTP:
            return RetryDecision(True, self.BACKOFF_SECONDS[min(attempt_number - 1, len(self.BACKOFF_SECONDS)-1)], "transient_http_error")
        if http_status is None and not meta_error_code:
            return RetryDecision(True, self.BACKOFF_SECONDS[min(attempt_number - 1, len(self.BACKOFF_SECONDS)-1)], "transport_error")
        return RetryDecision(False, 0, "non_retryable_error")
