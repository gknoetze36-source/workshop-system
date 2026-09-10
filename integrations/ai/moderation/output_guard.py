from __future__ import annotations

import re
from dataclasses import dataclass, field


# Money, without eating vehicle model designations.
#
# The previous pattern was (?:R|ZAR\s*)\s?([0-9][0-9\s,.]*), which matches "R8"
# in "Audi R8" and "R 1250" in "BMW R 1250 GS" -- both entirely ordinary things
# for a South African workshop customer to type, and each one refused a reply.
#
# What actually separates a rand amount from a model designation:
#   * Money is written with a thousands separator or decimal cents -- always a
#     price, whatever surrounds it.
#   * A model code is short (R8, R36) or is followed by another model token
#     (R 1250 GS, R1250GS). No workshop job costs under R100, so three or more
#     digits not trailed by a model token is a price.
#   * The exception to that exception: an explicit money word nearby (VAT, quote,
#     total) makes it a price again, so "R 3500 VAT incl" is still caught.
_MONEY_FORMATTED = re.compile(
    r"\b(?:R|ZAR)\s?\d{1,3}(?:[ ,]\d{3})+(?:\.\d{2})?\b"
    r"|\b(?:R|ZAR)\s?\d+\.\d{2}\b",
    re.IGNORECASE,
)
# Three or more digits, capturing whether a model-style token follows.
_MONEY_BARE = re.compile(r"\b(?:R|ZAR)\s?\d{3,}(?P<suffix>\s*[A-Z]{1,3}\b)?", re.IGNORECASE)
_MODEL_SUFFIX = re.compile(r"^\s*[A-Z]{1,3}$")
_PRICE_CONTEXT = re.compile(
    r"\b(?:cost|costs|costing|price|prices|priced|pricing|charge|charges|quote|quoted|"
    r"quotation|fee|fees|total|amount|estimate|invoice|pay|payment|payable|deposit|"
    r"rand|vat|incl|including|excl|excluding|discount|labour)\b",
    re.IGNORECASE,
)


def _mentions_a_price(text: str) -> bool:
    if _MONEY_FORMATTED.search(text):
        return True
    has_context = bool(_PRICE_CONTEXT.search(text))
    for match in _MONEY_BARE.finditer(text):
        suffix = match.group("suffix")
        # "R 1250 GS" reads as a model unless the message also talks money.
        if suffix and _MODEL_SUFFIX.match(suffix) and not has_context:
            continue
        return True
    return False

_SECRET_RE = re.compile(
    r"(?i)(?:sk-[A-Za-z0-9_-]{16,}|x-api-key\s*[:=]|api[_ -]?key\s*[:=]\s*\S+|authorization\s*:\s*bearer\s+\S+)"
)

# Claiming to be a person. Written affirmative-only so a correct disclosure --
# "I'm not a person, I'm the workshop's automated assistant" -- does not trip it.
_CLAIMS_HUMAN = re.compile(
    r"\b(?:i am|i'm|im)\s+(?:a\s+|an\s+|the\s+)?(?:real\s+|actual\s+|live\s+)?"
    r"(?:human|person|guy|lady|man|woman|technician|mechanic)\b",
    re.IGNORECASE,
)

# Telling a customer their vehicle is fine to drive. Affirmative-only, so
# "it is not safe to drive, please arrange a tow" is allowed and encouraged.
_SAFE_TO_DRIVE = re.compile(
    r"\b(?:is|it's|its|you're|you are|perfectly|totally|completely|quite)\s+"
    r"safe to drive\b",
    re.IGNORECASE,
)

# Liability, fault and insurance outcomes are for staff and insurers, never the bot.
_LIABILITY = re.compile(
    r"\b(?:your fault|their fault|not your fault|at fault|you are liable|you're liable|"
    r"we are liable|we're liable|we are not liable|we're not liable|"
    r"insurance will (?:cover|pay)|covered by (?:your )?insurance|"
    r"(?:your )?claim will be (?:approved|paid|covered))\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class GuardResult:
    allowed: bool
    reasons: list[str] = field(default_factory=list)


class OutputGuard:
    """Deterministic last-mile checks before AI text reaches a customer.

    A refusal here no longer raises into silence: AIConversationService turns a
    blocked reply into a human handoff plus a message the customer can actually
    see. That makes it safe for these checks to be strict.
    """

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
        lowered = text.lower()

        if not text.strip():
            reasons.append("AI returned an empty customer message")
        if len(text) > max_length:
            reasons.append(f"AI output exceeds maximum customer message length ({max_length})")
        if _SECRET_RE.search(text):
            reasons.append("AI output appears to contain a credential or authorization secret")

        if not booking_confirmation_recorded and any(
            phrase in lowered for phrase in ("your booking is confirmed", "booking is confirmed", "i have booked", "you are booked")
        ):
            reasons.append("booking confirmation cannot be asserted without a recorded customer booking confirmation")

        if approval_required and any(
            word in lowered
            for word in ("approved", "approval confirmed", "go ahead with the repair")
        ):
            reasons.append("approval language cannot be asserted without a recorded customer approval event")

        if _mentions_a_price(text):
            reasons.append("PHANTA does not provide workshop pricing")

        if _CLAIMS_HUMAN.search(text):
            reasons.append("the Service Advisor may not present itself as a human")

        if _SAFE_TO_DRIVE.search(text):
            reasons.append("the Service Advisor may not clear a vehicle as safe to drive")

        if _LIABILITY.search(text):
            reasons.append("the Service Advisor may not assign fault or promise insurance outcomes")

        return GuardResult(allowed=not reasons, reasons=reasons)
