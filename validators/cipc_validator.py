"""South African CIPC company registration number validation.

FORMAT
------
Modern CIPC registration numbers follow YYYY/NNNNNN/NN:

    YYYY      year of registration
    NNNNNN    six-digit sequence assigned within that year
    NN        two-digit entity-type code (07 private company, 06 public,
              08 non-profit, 21 personal liability, 23 close corporation)

Example: 2019/123456/07

SCOPE LIMIT -- READ THIS BEFORE CHANGING IT
-------------------------------------------
This validator accepts the MODERN format only, as decided during onboarding
design. It will therefore REJECT two categories of genuinely valid number:

  * pre-2002 registrations, which carry an alphabetical regional prefix
  * close corporations registered under the older "CK" numbering

Both remain valid on the CIPC register, and an established family workshop is
exactly the sort of customer likely to hold one. If a customer cannot get past
the business step of onboarding, this validator is the first thing to check:
relaxing it is a small, deliberate change rather than a bug fix.

Normalisation is forgiving (whitespace and separators), because rejecting a
correct number over a typed space is a bad first experience. Only the shape is
strict.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

# YYYY/NNNNNN/NN
CIPC_PATTERN = re.compile(r"^(\d{4})/(\d{6})/(\d{2})$")

# Earliest plausible registration year. Not a legal boundary -- a sanity check
# that catches transposed digits (e.g. 9201 instead of 2019).
MIN_YEAR = 1900

ENTITY_TYPE_LABELS = {
    "06": "Public company",
    "07": "Private company",
    "08": "Non-profit company",
    "09": "Company limited by guarantee",
    "10": "External company",
    "21": "Personal liability company",
    "22": "Unlimited company",
    "23": "Close corporation",
    "24": "Primary co-operative",
}


def normalise(value: str) -> str:
    """Tidy a submitted registration number without changing its meaning.

    Separators (spaces, hyphens, dashes, backslashes) are converted to "/"
    rather than deleted, because people copy these from letterheads and
    invoices in varying formats -- "2019 123456 07" and "2019-123456-07" are
    the same number. Deleting the separators instead of converting them would
    collapse the groups into one digit string and reject a valid number.
    """
    text = (value or "").strip().upper()
    text = re.sub(r"[\s\u2010-\u2015\\\-]+", "/", text)
    text = re.sub(r"/{2,}", "/", text)
    return text.strip("/")


def validate(value: str):
    """Validate a CIPC number.

    Returns (ok: bool, normalised: str, error: str | None). The caller decides
    whether to block on failure; the error text is written to be shown directly
    to the person filling in the form.
    """
    normalised = normalise(value)
    if not normalised:
        return False, "", "Enter your company registration number."

    match = CIPC_PATTERN.match(normalised)
    if not match:
        return False, normalised, (
            "Enter the registration number in the format 2019/123456/07 "
            "(year, six digits, entity-type code)."
        )

    year = int(match.group(1))
    current_year = datetime.now(timezone.utc).year
    if year < MIN_YEAR or year > current_year:
        return False, normalised, (
            f"The registration year {year} does not look right. "
            f"It should be between {MIN_YEAR} and {current_year}."
        )

    return True, normalised, None


def entity_type(value: str):
    """Return the entity-type label for a valid number, or None."""
    ok, normalised, _ = validate(value)
    if not ok:
        return None
    return ENTITY_TYPE_LABELS.get(normalised.split("/")[-1])
