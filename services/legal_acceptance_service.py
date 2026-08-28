"""Legal document acceptance records.

WHY NOT legal_accepted = true
-----------------------------
A single boolean cannot answer the questions that actually get asked when an
acceptance is disputed or audited:

  * WHICH document did they accept? (terms, privacy notice, DPA)
  * WHICH VERSION? Documents change. An acceptance of v1 is not an acceptance
    of v2, and "they ticked a box once" is not evidence of agreeing to text
    that was published afterwards.
  * WHO accepted it, and on behalf of WHICH business?
  * WHEN, and BY WHAT MEANS?
  * Have ALL currently-required documents been accepted, or only some?

So each acceptance is its own row: one document, one version, one acceptor,
one timestamp. Re-acceptance after a version bump creates a NEW row rather
than overwriting the old one -- the history is the evidence.

DOCUMENT REGISTRY
-----------------
REQUIRED_DOCUMENTS below is the source of truth for what must be accepted and
at which version. Bumping a version here means existing users have not
accepted the new version, and outstanding_documents() will say so. That is
intentional: it is how a re-acceptance prompt gets triggered.

The version strings are deliberately dates rather than v1/v2 so an acceptance
row states plainly which published text was agreed to.
"""
from __future__ import annotations

import logging

from database import query_db, execute_db, utc_now

logger = logging.getLogger(__name__)

# Documents a workshop must accept, and the version currently in force.
#
# IMPORTANT: these versions must match the documents actually published on the
# PHANTA site. Update them together, never separately -- a version recorded
# here that does not correspond to published text makes the acceptance record
# meaningless.
REQUIRED_DOCUMENTS = {
    "terms_of_service": "2026-08-28",
    "privacy_policy": "2026-08-28",
    "data_processing_agreement": "2026-08-28",
    "billing_payment_terms": "2026-08-28",
    "acceptable_use_policy": "2026-08-28",
}

DOCUMENT_LABELS = {
    "terms_of_service": "Terms of Service",
    "privacy_policy": "Privacy Policy",
    "data_processing_agreement": "Data Processing Agreement",
    "billing_payment_terms": "Billing & Payment Terms",
    "acceptable_use_policy": "Acceptable Use Policy",
}

# Source file for each document, read at request time and rendered into the
# onboarding modal. Kept as files rather than database rows so the published
# text is version-controlled alongside the code that gates on its version.
DOCUMENT_FILES = {
    "terms_of_service": "01-Terms-of-Service.md",
    "privacy_policy": "02-Privacy-Policy.md",
    "data_processing_agreement": "03-Data-Processing-Agreement.md",
    "billing_payment_terms": "04-Billing-Payment-Terms.md",
    "acceptable_use_policy": "05-Acceptable-Use-Policy.md",
}

# Display order in the onboarding legal step.
DOCUMENT_ORDER = (
    "terms_of_service",
    "privacy_policy",
    "data_processing_agreement",
    "billing_payment_terms",
    "acceptable_use_policy",
)


def document_text(document_key: str) -> str:
    """Return the full published text of one legal document.

    Read from disk on request so the document shown to the customer is always
    the text in the repository at the deployed version. Never truncated -- the
    modal is scrollable.
    """
    import os

    filename = DOCUMENT_FILES.get(document_key)
    if not filename:
        raise LookupError(f"unknown legal document: {document_key}")
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(root, "legal_documents", filename)
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()

# How the acceptance was given.
METHOD_ONBOARDING_CHECKBOX = "onboarding_checkbox"
METHOD_REACCEPTANCE_PROMPT = "reacceptance_prompt"


def record_acceptance(
    *,
    document_key: str,
    version: str,
    user_id,
    location_id,
    owner_id=None,
    method: str = METHOD_ONBOARDING_CHECKBOX,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> bool:
    """Record one document acceptance. Returns False if already recorded."""
    # Acceptance is given by the BUSINESS, so uniqueness is keyed on the owner
    # where one is known. Falling back to location keeps older records working.
    if owner_id:
        existing = query_db(
            """
            SELECT id FROM legal_acceptances
            WHERE document_key=%s AND document_version=%s AND owner_id=%s
            LIMIT 1
            """,
            (document_key, version, owner_id),
            one=True,
        )
    else:
        existing = query_db(
            """
            SELECT id FROM legal_acceptances
            WHERE document_key=%s AND document_version=%s
              AND user_id=%s AND location_id=%s
            LIMIT 1
            """,
            (document_key, version, user_id, location_id),
            one=True,
        )
    if existing:
        return False

    execute_db(
        """
        INSERT INTO legal_acceptances (
            document_key, document_version, document_label,
            user_id, owner_id, location_id, method, ip_address, user_agent, accepted_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            document_key,
            version,
            DOCUMENT_LABELS.get(document_key, document_key),
            user_id,
            owner_id,
            location_id,
            method,
            ip_address,
            (user_agent or "")[:255] or None,
            utc_now(),
        ),
    )

    from helpers.audit import record_audit

    record_audit(
        "legal.document_accepted",
        "legal_acceptance",
        entity_id=document_key,
        location_id=location_id,
        user_id=user_id,
        details={"document": document_key, "version": version, "method": method},
    )
    logger.info(
        "legal_acceptance_recorded document=%s version=%s user_id=%s location_id=%s",
        document_key, version, user_id, location_id,
    )
    return True


def accepted_documents(user_id, location_id, owner_id=None):
    """Return {document_key: version} accepted by this business.

    Keyed on the owner where known, because acceptance belongs to the business
    rather than to the individual who happened to click, or to one branch.
    """
    if owner_id:
        rows = query_db(
            "SELECT document_key, document_version FROM legal_acceptances WHERE owner_id=%s",
            (owner_id,),
        ) or []
    else:
        rows = query_db(
            """
            SELECT document_key, document_version
            FROM legal_acceptances
            WHERE user_id=%s AND location_id=%s
            """,
            (user_id, location_id),
        ) or []
    return {r["document_key"]: r["document_version"] for r in rows}


def outstanding_documents(user_id, location_id, owner_id=None):
    """Return the documents still needing acceptance at the current version.

    A document counts as outstanding when it has never been accepted OR when
    the accepted version is not the version currently in force.
    """
    accepted = accepted_documents(user_id, location_id, owner_id=owner_id)
    return {
        key: version
        for key, version in REQUIRED_DOCUMENTS.items()
        if accepted.get(key) != version
    }


def has_accepted_all(user_id, location_id, owner_id=None) -> bool:
    """True when every required document is accepted at its current version."""
    return not outstanding_documents(user_id, location_id, owner_id=owner_id)


def record_all_required(
    *, user_id, location_id, owner_id=None, method=METHOD_ONBOARDING_CHECKBOX,
    ip_address=None, user_agent=None,
):
    """Record acceptance of every currently-outstanding required document.

    Returns the list of document keys that were newly recorded. Each becomes
    its own row, so a later version bump leaves this history intact.
    """
    recorded = []
    for key, version in outstanding_documents(user_id, location_id, owner_id=owner_id).items():
        if record_acceptance(
            document_key=key, version=version, user_id=user_id,
            location_id=location_id, owner_id=owner_id, method=method,
            ip_address=ip_address, user_agent=user_agent,
        ):
            recorded.append(key)
    return recorded
