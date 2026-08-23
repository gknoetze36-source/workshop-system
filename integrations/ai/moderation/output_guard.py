from __future__ import annotations

import re
from dataclasses import dataclass, field


_PRICE_RE = re.compile(r"(?:R|ZAR\s*)\s?([0-9][0-9\s,.]*)", re.IGNORECASE)
_SECRET_RE = re.compile(
    r"(?i)(?:sk-[A-Za-z0-9_-]{16,}|x-api-key\s*[:=]|api[_ -]?key\s*[:=]\s*\S+|authorization\s*:\s*bearer\s+\S+)"
)


@dataclass(frozen=True, slots=True)
class GuardResult:
    allowed: bool
    reasons: list[str] = field(default_factory=list)


class OutputGuard:
    """Deterministic last-mile checks before AI text reaches a customer."""

    def validate(
        self,
        text: str,
        *,
        allowed_prices: list[float | int] | None = None,
        approval_required: bool = False,
        booking_confirmation_recorded: bool = False,
        max_length: int = 4000,
    ) -> GuardResult:
        reasons: list[str] = []
        text = text or ""

        if not text.strip():
            reasons.append("AI returned an empty customer message")
        if len(text) > max_length:
            reasons.append(f"AI output exceeds maximum customer message length ({max_length})")
        if _SECRET_RE.search(text):
            reasons.append("AI output appears to contain a credential or authorization secret")

        if not booking_confirmation_recorded and any(
            phrase in text.lower() for phrase in ("your booking is confirmed", "booking is confirmed", "i have booked", "you are booked")
        ):
            reasons.append("booking confirmation cannot be asserted without a recorded customer booking confirmation")

        if approval_required and any(
            word in text.lower()
            for word in ("approved", "approval confirmed", "go ahead with the repair")
        ):
            reasons.append("approval language cannot be asserted without a recorded customer approval event")

        if _PRICE_RE.search(text):
            reasons.append("PHANTA does not provide workshop pricing")

        return GuardResult(allowed=not reasons, reasons=reasons)
