# PHANTA — Four Production Fixes

This pass is intentionally limited to the four issues identified in the previous audit.

## 1. Tenant RLS context

- `database/sqlalchemy_session.py`: `get_session()` now binds the authenticated Flask request's `g.tenant_id` to the PostgreSQL transaction with `set_tenant_id()`.
- `database/connection.py`: legacy/raw SQL connections now bind both `app.tenant_id` and the platform-admin read context for the current request.
- Background/provider code without a tenant is not given a guessed tenant. It must resolve the tenant explicitly before accessing tenant-owned records.

## 2. Provider webhook ingress

### Meta

- A signed Meta webhook first resolves its WABA/phone-number mapping using the read-only platform context.
- The resolved tenant is then processed inside `tenant_transaction(tenant_id)` so all webhook writes and reads run under normal tenant RLS.
- The Service Advisor follow-up uses the same tenant transaction context.
- The Meta webhook result now carries `tenant_id` for downstream processing.

### Paystack

- A signed Paystack event first resolves the tenant using a read-only platform context.
- The event is then handled inside `tenant_transaction(tenant_id)`.
- The existing handler receives the resolved tenant explicitly, avoiding an RLS-blocked pre-resolution query.
- Tenant resolution logic is centralized in `integrations/paystack/webhooks/webhook_location_resolver.py`.

## 3. Provider webhook CSRF

- Meta and Paystack webhook blueprints are explicitly exempt from browser CSRF because they authenticate using Meta/Paystack cryptographic signatures.
- Browser-originated state-changing requests remain CSRF protected.
- The existing request-level protection remains in place for non-provider state-changing requests.

## 4. Evidence-only data rule

This pass does not add fake data or defaults. Existing Audit Centre data remains evidence-driven. Missing data is not inferred from another field.

## Validation

- Targeted RLS/webhook tests: 5/5 PASS
- Meta/Paystack webhook integration tests: 19/19 PASS
- Phase 12 validation: 9/9 PASS
- Pre-GitHub gate: PASS
- Python syntax compilation: PASS
- Release archive cleaned of generated `__pycache__`, `.pyc`, and `.pytest_cache` files.

## Full-suite limitation

A complete `pytest -q` run could not finish in the available environment because the environment is missing Flask while collecting `tests/unit/test_flyer_lady.py`. This is recorded rather than being represented as a passing full-suite result.
