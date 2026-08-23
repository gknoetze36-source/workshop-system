# PHANTA Audit Fix Report — 2026-08-11

## Scope

This release is a focused correction pass against the supplied PHANTA workshop build. Existing workshop/business logic was not rewritten. Changes are limited to the audit/platform read path, PostgreSQL RLS context, CSRF handling, production migration startup, release hygiene, and the specific security findings identified in the audit.

## Fixed

1. **Platform-admin PostgreSQL RLS context**
   - Added `get_platform_session()`.
   - It sets `app.platform_admin=1` only for the current PostgreSQL transaction.
   - Added migration `0012_platform_admin_read_policy.py` with SELECT-only platform policies.
   - Tenant policies remain in place; platform admins do not receive tenant write access.
   - `flyer_lady_public_links` is now included in tenant RLS because the previous migration created it without RLS.

2. **Client Audit Centre**
   - Added a read-only platform client list and per-client evidence view.
   - Added JSON evidence endpoint.
   - No access tokens, app secrets, or raw webhook payloads are exposed.
   - Missing records remain missing instead of being replaced with guessed statuses.
   - Client list uses bulk queries for connection/subscription summaries instead of one query per client.

3. **CSRF protection**
   - Restored default CSRF protection for state-changing requests, including JSON requests.
   - Flyer Lady JavaScript sends the CSRF token in `X-CSRFToken`.
   - External Meta and Paystack webhooks are explicitly excluded because they authenticate with provider signatures rather than browser CSRF tokens.

4. **Production migrations**
   - Docker startup now runs `alembic upgrade head` before Gunicorn.
   - This keeps production schema changes owned by Alembic rather than relying on `create_all()`.

5. **Release hygiene**
   - Removed `.pyc`, `__pycache__`, and pytest cache artifacts from the release.

6. **Regression checks**
   - Added focused platform-admin security tests.
   - Existing Phase 12 validation remains passing.
   - Pre-GitHub validation passes.
   - Python compilation passes.

## Intentionally not changed

The following were not altered in this focused pass:

- Ready-for-collection duplicate-send behavior.
- Global customer/vehicle search endpoint.
- Backend PHANTA Ghost assistant architecture beyond existing code.
- Workshop business workflows unrelated to the audit/security findings.

## Verification completed

- `python -m compileall -q .` — PASS
- `pytest -q tests/unit/test_platform_admin_security.py` — 4 PASS
- `python scripts/validate_phase12.py` — 9 PASS
- `python scripts/pre_github_check.py` — PASS
- `alembic heads` — `0012_platform_admin_read_policy`

A full application pytest run still requires the project's Python dependencies to be installed in the execution environment; that is an environment limitation, not a claimed application test result.
