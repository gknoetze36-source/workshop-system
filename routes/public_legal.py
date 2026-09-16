"""Public, unauthenticated legal document pages.

Two routes, both genuinely public -- reachable while completely logged
out, no session, no location context:

    GET /privacy-policy
    GET /terms-of-service

These exist specifically so Meta's App Review can be pointed at a real,
public URL for each, rather than the existing /onboarding/legal route,
which is @login_required and correctly stays that way -- it is where a
workshop *accepts* these documents, a different job from *displaying*
them to the public.

Source of truth: document_text() in services/legal_acceptance_service.py,
the same function the onboarding acceptance flow already uses to read
legal_documents/*.md. Reused here rather than re-implemented, so there is
exactly one place that knows where the published text lives.

This module does not edit the legal text in any way. The source
markdown files (legal_documents/01-Terms-of-Service.md and
02-Privacy-Policy.md) still contain multiple unresolved placeholders --
[Registration Number], [Registered Address], [Effective Date], phone
numbers, notice periods, a liability cap, POPIA Information Officer
details, and data-retention periods -- none of which exist anywhere
else in this repository. Inventing any of them would be fabricating
legal content, so they are rendered exactly as written, brackets and
all. [Company Legal Name] was the one exception: resolved to
"Vanta Automations (Pty) Ltd" on 2026-09-16, stated directly by the
operator, across all five legal_documents files and with the
corresponding version bump in services/legal_acceptance_service.py.
See the accompanying implementation report for the complete remaining
list; a human with legal authority over VANTA Automations needs to
resolve them before these URLs are treated as final, and before
either document is what Meta's App Review actually sees represented as
finished.
"""
from __future__ import annotations

import markdown as markdown_lib
from flask import Blueprint, render_template

from services.legal_acceptance_service import document_text

public_legal_bp = Blueprint("public_legal", __name__)

_MARKDOWN_EXTENSIONS = ["tables", "sane_lists"]


def _render_markdown(document_key: str) -> str:
    """Read the published document and convert it to HTML.

    No text substitution happens here -- see the module docstring. This
    only does the markdown-to-HTML conversion so headings, lists and
    tables in the source render properly instead of showing as raw
    asterisks and hyphens on the page.
    """
    text = document_text(document_key)
    return markdown_lib.markdown(text, extensions=_MARKDOWN_EXTENSIONS)


@public_legal_bp.get("/privacy-policy")
def privacy_policy():
    html = _render_markdown("privacy_policy")
    return render_template("public_legal_document.html", title="Privacy Policy", document_html=html)


@public_legal_bp.get("/terms-of-service")
def terms_of_service():
    html = _render_markdown("terms_of_service")
    return render_template("public_legal_document.html", title="Terms of Service", document_html=html)
