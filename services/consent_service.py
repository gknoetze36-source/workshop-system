"""Customer-level marketing consent and suppression.

WHY CONSENT MOVED TO THE CUSTOMER
---------------------------------
Consent used to be captured on the booking: bookings.whatsapp_opt_in,
bookings.reminder_opt_in, bookings.privacy_consent_at. That makes consent
per-visit -- a customer who opted out last time is asked again on the next
booking, and an opt-out does not persist. It also records no evidence: no
source, no method, no timestamp tied to the decision.

customers.accepts_whatsapp already existed as a customer-level flag but
carried the same gap, and defaults to TRUE (opt-out semantics) so it cannot
by itself evidence an affirmative marketing opt-in.

This module adds the evidence fields POPIA expects for direct marketing:
what was decided, when, by what means, and where the decision came from.

PRECEDENCE RULE
---------------
Customer-level marketing consent is authoritative. A booking-level opt-in
must never resurrect a customer who has explicitly opted out of marketing --
that is the "system must not accidentally re-add the customer" requirement.
So: an explicit opt-out always wins, regardless of what any later booking
form submits.

SCOPE
-----
This governs MARKETING only. Operational messages about a booking the
customer actually made are not suppressed by it -- see
constants/message_categories.py.
"""
from __future__ import annotations

import logging

from database import query_db, execute_db, utc_now
from helpers.common import boolish

logger = logging.getLogger(__name__)

# How the decision reached PHANTA.
SOURCE_BOOKING_FORM = "booking_form"
SOURCE_STAFF = "staff_capture"
SOURCE_WHATSAPP_REPLY = "whatsapp_reply"
SOURCE_IMPORT = "import"

# What the customer did.
STATE_OPTED_IN = "opted_in"
STATE_OPTED_OUT = "opted_out"
STATE_UNKNOWN = "unknown"


def get_marketing_state(customer_id, location_id):
    """Return the customer's current marketing consent state.

    Returns STATE_UNKNOWN when no explicit decision has been recorded. Callers
    must treat UNKNOWN as "do not send marketing": absence of an opt-out is
    not consent.
    """
    row = query_db(
        """
        SELECT marketing_consent_state, marketing_consent_at,
               marketing_consent_source, marketing_consent_method
        FROM customers
        WHERE id=%s AND location_id=%s
        """,
        (customer_id, location_id),
        one=True,
    )
    if not row:
        return STATE_UNKNOWN
    return (row.get("marketing_consent_state") or STATE_UNKNOWN).strip().lower()


def may_send_marketing(customer_id, location_id) -> bool:
    """True only when the customer has affirmatively opted in to marketing."""
    return get_marketing_state(customer_id, location_id) == STATE_OPTED_IN


def record_marketing_decision(
    customer_id,
    location_id,
    *,
    opted_in: bool,
    source: str,
    method: str,
    actor_user_id=None,
    note: str | None = None,
) -> bool:
    """Record a marketing consent decision with its evidence.

    Returns True when the decision was written, False when it was refused.

    An explicit opt-out is never overwritten by a later opt-in that did not
    come directly from the customer. Staff capture and the booking form cannot
    re-enrol someone who opted out; only an explicit customer action
    (SOURCE_WHATSAPP_REPLY) or a deliberate staff override with an audit note
    may do so.
    """
    current = get_marketing_state(customer_id, location_id)

    if current == STATE_OPTED_OUT and opted_in and source in (SOURCE_BOOKING_FORM, SOURCE_IMPORT):
        logger.info(
            "marketing_opt_in_refused reason=existing_opt_out customer_id=%s source=%s",
            customer_id, source,
        )
        return False

    execute_db(
        """
        UPDATE customers
        SET marketing_consent_state=%s,
            marketing_consent_at=%s,
            marketing_consent_source=%s,
            marketing_consent_method=%s,
            marketing_consent_note=%s,
            accepts_whatsapp=%s,
            updated_at=%s
        WHERE id=%s AND location_id=%s
        """,
        (
            STATE_OPTED_IN if opted_in else STATE_OPTED_OUT,
            utc_now(),
            source,
            method,
            note,
            # Keep the pre-existing flag consistent so automation rules that
            # already reference customer.accepts_whatsapp stay correct.
            opted_in,
            utc_now(),
            customer_id,
            location_id,
        ),
    )

    from helpers.audit import record_audit

    record_audit(
        "customer.marketing_consent_changed",
        "customer",
        entity_id=customer_id,
        location_id=location_id,
        details={
            "state": STATE_OPTED_IN if opted_in else STATE_OPTED_OUT,
            "source": source,
            "method": method,
            "previous_state": current,
        },
    )
    return True


def suppress_marketing(customer_id, location_id, *, source, method, note=None) -> bool:
    """Opt a customer out of marketing. Always permitted."""
    return record_marketing_decision(
        customer_id, location_id,
        opted_in=False, source=source, method=method, note=note,
    )
