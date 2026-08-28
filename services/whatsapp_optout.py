"""Marketing opt-out and re-subscribe handling for inbound WhatsApp messages.

WHAT THIS DOES AND DELIBERATELY DOES NOT DO
-------------------------------------------
A customer replying STOP is opting out of MARKETING. It is not a request to
stop being told that their vehicle is ready, or that their booking is
tomorrow. Treating STOP as "block all future communication" would break the
service the workshop is actually being paid to provide, and would leave the
customer without the operational messages they arranged to receive.

So an opt-out here calls services/consent_service.suppress_marketing(), which
sets customer-level marketing state only. Operational categories in
constants/message_categories.py are unaffected -- see
services/messaging_service.can_send_outbound().

TWO SIGNALS, NOT ONE
--------------------
Marketing-category templates carry Meta's own stop/resume buttons. When a
customer taps one, the signal reaches this webhook rather than arriving as
the literal word "STOP". If only text replies were processed, PHANTA's
database would drift out of agreement with Meta: marketing would keep being
queued for customers Meta has already unsubscribed, which harms the WABA
quality rating even though nothing is delivered.

Both paths therefore resolve to the same consent write.

KEYWORD CHOICE
--------------
STOP and UNSUBSCRIBE are the conventional pair -- customers already know them
from SMS. QUIT, END and CANCEL are accepted as common variants. Matching is
whole-message and case-insensitive: a message that merely contains the word
"stop" ("please stop by at 9am", "my car won't stop making that noise") must
NOT unsubscribe anybody, which is why substring matching is not used.
"""
from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# Whole-message matches only. Punctuation and surrounding whitespace are
# tolerated; embedded occurrences are not.
OPT_OUT_KEYWORDS = frozenset({"stop", "unsubscribe", "quit", "end", "cancel"})
OPT_IN_KEYWORDS = frozenset({"start", "resume", "subscribe", "unstop"})

_NORMALISE = re.compile(r"[^a-z]+")


def _normalise(text: str) -> str:
    """Reduce a message to bare letters for whole-message keyword matching."""
    return _NORMALISE.sub("", (text or "").strip().lower())


def classify_inbound(text: str) -> str | None:
    """Return 'opt_out', 'opt_in', or None for an inbound message body."""
    token = _normalise(text)
    if not token or len(token) > 20:
        # Longer messages are conversation, not a keyword. Guards against a
        # sentence collapsing into something that happens to match.
        return None
    if token in OPT_OUT_KEYWORDS:
        return "opt_out"
    if token in OPT_IN_KEYWORDS:
        return "opt_in"
    return None


def process_consent_keyword(
    *,
    customer_id,
    location_id,
    text,
    native_signal=None,
):
    """Apply an opt-out/opt-in if the inbound message is one.

    `native_signal` carries Meta's own marketing stop/resume indication where
    the payload provides it, so a button tap is handled identically to a typed
    keyword.

    Returns a dict describing what happened, or None when the message was
    ordinary conversation and should continue to the Service Advisor.
    """
    action = native_signal or classify_inbound(text)
    if action not in ("opt_out", "opt_in"):
        return None

    if not customer_id or not location_id:
        logger.warning(
            "consent_keyword_unresolved action=%s customer_id=%s location_id=%s",
            action, customer_id, location_id,
        )
        return None

    from services.consent_service import (
        record_marketing_decision,
        suppress_marketing,
        SOURCE_WHATSAPP_REPLY,
    )

    method = "meta_native_button" if native_signal else f"whatsapp_reply:{_normalise(text)}"

    if action == "opt_out":
        suppress_marketing(
            customer_id, location_id,
            source=SOURCE_WHATSAPP_REPLY, method=method,
        )
        logger.info("marketing_opt_out_processed customer_id=%s location_id=%s", customer_id, location_id)
        return {"action": "opt_out", "method": method}

    # An explicit inbound re-subscribe is a direct customer action, which is
    # the one route consent_service permits to reverse a prior opt-out.
    record_marketing_decision(
        customer_id, location_id,
        opted_in=True, source=SOURCE_WHATSAPP_REPLY, method=method,
    )
    logger.info("marketing_opt_in_processed customer_id=%s location_id=%s", customer_id, location_id)
    return {"action": "opt_in", "method": method}


def confirmation_text(action: str, workshop_name: str | None = None) -> str:
    """Wording for the automatic confirmation reply.

    The operational sentence is not decoration. A bare "you have been
    unsubscribed" would misrepresent what happened: the customer still
    receives booking confirmations, reminders and vehicle-ready messages, and
    telling them so is what keeps the marketing/operational distinction that
    the consent model rests on honest.
    """
    business = workshop_name or "this workshop"
    if action == "opt_out":
        return (
            f"You've been unsubscribed from marketing messages from {business}. "
            "You'll still receive messages about your bookings and vehicle, "
            "such as confirmations, reminders and collection notices. "
            "Reply START to receive offers again."
        )
    return (
        f"You're subscribed to marketing messages from {business} again. "
        "Reply STOP at any time to unsubscribe."
    )
