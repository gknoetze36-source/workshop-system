"""Public legal document pages -- must be reachable while genuinely
logged out, per the spec that introduced them.

Covers /privacy-policy, /terms-of-service (routes/public_legal.py) and
/data-deletion (routes/data_deletion.py, pre-existing -- verified here
rather than assumed, since this test file is what should have caught a
regression if adding the new blueprint had broken it).
"""
from __future__ import annotations

import re

import pytest


PUBLIC_LEGAL_PATHS = ("/privacy-policy", "/terms-of-service", "/data-deletion")


@pytest.fixture
def anon_client():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    return phanta_app.app.test_client()


@pytest.mark.parametrize("path", PUBLIC_LEGAL_PATHS)
def test_public_legal_page_returns_200_when_logged_out(anon_client, path):
    response = anon_client.get(path)
    assert response.status_code == 200


@pytest.mark.parametrize("path", PUBLIC_LEGAL_PATHS)
def test_public_legal_page_does_not_redirect_to_login(anon_client, path):
    response = anon_client.get(path, follow_redirects=False)
    location = response.headers.get("Location", "")
    assert "login" not in location.lower()


@pytest.mark.parametrize("path", PUBLIC_LEGAL_PATHS)
def test_public_legal_page_renders_real_html(anon_client, path):
    body = anon_client.get(path).get_data(as_text=True)
    assert "<h1" in body.lower() or "<html" in body.lower()
    assert len(body) > 500


def test_privacy_policy_has_correct_title(anon_client):
    body = anon_client.get("/privacy-policy").get_data(as_text=True)
    assert "<title>Privacy Policy" in body


def test_terms_of_service_has_correct_title(anon_client):
    body = anon_client.get("/terms-of-service").get_data(as_text=True)
    assert "<title>Terms of Service" in body


def test_privacy_policy_renders_the_actual_source_document(anon_client):
    """Confirms this route genuinely renders legal_documents/02-Privacy-Policy.md
    -- not a placeholder page, not a different document."""
    body = anon_client.get("/privacy-policy").get_data(as_text=True)
    assert "POPIA" in body
    # legal_documents/02-Privacy-Policy.md now uses "Responsible Party" (the
    # correct POPIA term for the organisation, analogous to GDPR's "Data
    # Controller") rather than naming an "Information Officer" -- a
    # separate, specific POPIA concept (a designated individual registered
    # with the Information Regulator, POPIA s55). This document was
    # deliberately rewritten by a human operator since this assertion was
    # written (confirmed: most of the document's placeholders were also
    # resolved in the same edit, not left as stale artifacts). Whether a
    # named Information Officer still needs to be designated and disclosed
    # is a real, separate compliance question this test cannot answer --
    # flagged to the founder rather than silently dropped or assumed
    # resolved. This test's only job is confirming the route renders the
    # actual current source document, which it does.
    assert "Responsible Party" in body


def test_legal_pages_state_the_real_company_name(anon_client):
    """Company Legal Name was resolved 2026-09-16, authorized directly by
    the operator. Confirms it actually appears, and that the old bracket
    placeholder is genuinely gone from both pages."""
    for path in ("/privacy-policy", "/terms-of-service"):
        body = anon_client.get(path).get_data(as_text=True)
        assert "Vanta Automations (Pty) Ltd" in body
        assert "[Company Legal Name]" not in body


def test_terms_of_service_renders_the_actual_source_document(anon_client):
    body = anon_client.get("/terms-of-service").get_data(as_text=True)
    assert "Republic of South Africa" in body


def test_public_legal_pages_do_not_leak_secrets_or_internals(anon_client):
    """No environment variable name, stack trace, or internal path should
    ever appear on a page served to the public internet."""
    for path in PUBLIC_LEGAL_PATHS:
        body = anon_client.get(path).get_data(as_text=True)
        for forbidden in ("Traceback", "SECRET_KEY", "DATABASE_URL", "/app/", "Werkzeug Debugger"):
            assert forbidden not in body, f"{path} leaked {forbidden!r}"


def test_privacy_policy_unresolved_placeholders_are_the_known_set(anon_client):
    """Documents exactly which legal placeholders remain unresolved, so a
    future change that accidentally resolves (or newly introduces) one is
    visible as a deliberate test update, not a silent diff.

    This is not a claim that these placeholders are acceptable to publish
    -- see the accompanying implementation report. It only pins the
    current, known set so regressions are caught.
    """
    body = anon_client.get("/privacy-policy").get_data(as_text=True)
    legal_brackets = {
        m for m in re.findall(r"\[[^\]\n]{2,300}\]", body)
        if not m.startswith("[type=") and not m.startswith("[aria-")
        and not m.startswith("[data-")
    }
    # The document was subsequently rewritten by a human operator and every
    # previously-known placeholder below was genuinely resolved with real
    # values -- confirmed empty, not assumed. Pinned to the empty set for
    # the same reason the original comment gives: so a future change that
    # newly introduces a placeholder is caught as a deliberate test update.
    expected = set()
    assert legal_brackets == expected, (
        f"Privacy Policy placeholder set changed. New: {legal_brackets - expected}, "
        f"resolved: {expected - legal_brackets}"
    )


def test_terms_of_service_unresolved_placeholders_are_the_known_set(anon_client):
    body = anon_client.get("/terms-of-service").get_data(as_text=True)
    legal_brackets = {
        m for m in re.findall(r"\[[^\]\n]{2,300}\]", body)
        if not m.startswith("[type=") and not m.startswith("[aria-")
        and not m.startswith("[data-")
    }
    # Unlike the Privacy Policy (fully resolved), the Terms of Service was
    # only partially rewritten -- these three genuinely remain unresolved
    # in the current document, confirmed by actually reading the response
    # body rather than assuming both documents were finished together.
    expected = {"[Governing City/Province]", "[3/6]", "[14/30]"}
    assert legal_brackets == expected, (
        f"Terms of Service placeholder set changed. New: {legal_brackets - expected}, "
        f"resolved: {expected - legal_brackets}"
    )


def test_onboarding_legal_acceptance_route_remains_login_protected():
    """The spec is explicit that the protected /onboarding/legal route must
    keep its login requirement -- these new public pages are an addition,
    not a replacement, and must not have loosened it."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    response = client.get("/onboarding/legal", follow_redirects=False)
    assert response.status_code in (302, 401)
    if response.status_code == 302:
        assert "login" in response.headers.get("Location", "").lower()


def test_markdown_dependency_is_declared_in_requirements():
    """S-level regression guard for exactly today's failure mode: a module
    that imports a package not listed in requirements.txt works locally
    (already installed in the dev/CI image) and crashes production on a
    fresh build. Confirmed the hard way earlier this session."""
    import pathlib
    requirements = (pathlib.Path(__file__).resolve().parents[2] / "requirements.txt").read_text()
    assert "markdown" in requirements.lower()
