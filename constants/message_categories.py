"""Outbound message categories.

WHY CATEGORIES EXIST
--------------------
Marketing suppression must not block operational messages. A customer who
opts out of marketing is still entitled to be told their vehicle is ready.

Before this module, the only categorisation was a substring test in
services/messaging_service.py::can_send_outbound:

    if not booking["reminder_opt_in"] and "reminder" in subject.lower():

That is fragile (it depends on wording, and breaks on a translated or
reworded subject) and it has no concept of marketing at all. Every send path
now declares a category from this module instead.

OPERATIONAL vs MARKETING
------------------------
OPERATIONAL categories relate to a booking or job the customer has actually
arranged with the workshop. They are sent because a service relationship
exists, and are not suppressed by a marketing opt-out.

MARKETING is promotional content sent to win further business. It is
suppressed whenever the customer's marketing consent is not affirmatively
granted.

REMINDER sits in OPERATIONAL but has its own opt-out (reminder_opt_in), which
predates this module and is preserved.
"""

BOOKING_CONFIRMATION = "BOOKING_CONFIRMATION"
BOOKING_REMINDER = "BOOKING_REMINDER"
BOOKING_CANCELLED = "BOOKING_CANCELLED"
VEHICLE_READY = "VEHICLE_READY"
SERVICE_FOLLOWUP = "SERVICE_FOLLOWUP"
QUOTE = "QUOTE"
INVOICE = "INVOICE"
REVIEW_REQUEST = "REVIEW_REQUEST"
MARKETING = "MARKETING"

# Messages sent because a service relationship exists. Not suppressed by a
# marketing opt-out.
OPERATIONAL_CATEGORIES = frozenset({
    BOOKING_CONFIRMATION,
    BOOKING_REMINDER,
    BOOKING_CANCELLED,
    VEHICLE_READY,
    SERVICE_FOLLOWUP,
    QUOTE,
    INVOICE,
})

# Categories that require affirmative marketing consent.
MARKETING_CATEGORIES = frozenset({MARKETING})

# A review request is promotional in some jurisdictions and operational in
# others. It is treated as marketing here because that is the conservative
# reading: suppressing it is a lost review, sending it wrongly is a complaint.
MARKETING_CATEGORIES = MARKETING_CATEGORIES | {REVIEW_REQUEST}

# Categories governed by the pre-existing per-booking reminder opt-out.
REMINDER_CATEGORIES = frozenset({BOOKING_REMINDER})

ALL_CATEGORIES = OPERATIONAL_CATEGORIES | MARKETING_CATEGORIES

# Used when a legacy call site has not yet declared a category. Treated as
# operational so that unlabelled existing traffic is not silently dropped,
# but it is logged so the remaining call sites can be found and labelled.
UNCATEGORISED = "UNCATEGORISED"


def is_marketing(category) -> bool:
    """True when *category* requires affirmative marketing consent."""
    return (category or UNCATEGORISED) in MARKETING_CATEGORIES


def is_reminder(category) -> bool:
    """True when *category* is governed by the per-booking reminder opt-out."""
    return (category or UNCATEGORISED) in REMINDER_CATEGORIES


def normalise_category(category) -> str:
    """Return a known category name, or UNCATEGORISED."""
    value = (category or "").strip().upper()
    return value if value in ALL_CATEGORIES else UNCATEGORISED
