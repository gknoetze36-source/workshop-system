# PHANTA Final Focused Audit — 2026-08-11

## Scope
This pass is limited to the six requested actions:
1. Remove remaining Client Audit Centre placeholders.
2. Add a real PostgreSQL RLS integration test with two tenants and SuperAdmin.
3. Add a real Meta webhook tenant-resolution integration test.
4. Add a real Paystack webhook tenant-resolution integration test.
5. Run those tests against PostgreSQL.
6. Rerun the complete available audit.

## Changes made
- Client Audit Centre no longer renders `No record`, `—`, or equivalent fallback values for missing evidence. Empty evidence fields/sections are omitted.
- Added `integrations/meta/webhook/webhook_location_resolver.py` so Meta tenant resolution is isolated, testable, and explicitly runs before tenant RLS processing.
- Added `tests/integration_postgres_rls_webhooks.py` covering:
  - Tenant A sees only Tenant A under PostgreSQL RLS.
  - Tenant B sees only Tenant B under PostgreSQL RLS.
  - Platform-admin read context sees both tenants.
  - Meta WABA/phone-number resolution occurs in platform read context and the event is then processed under tenant RLS.
  - Paystack customer-code resolution occurs in platform read context and the webhook event is then processed under tenant RLS.
- Tests are intentionally gated by `PHANTA_TEST_DATABASE_URL` and reject SQLite URLs. They use one transaction and roll it back.

## Validation results
- Alembic head: `0012_platform_admin_read_policy` — PASS
- Audit Centre placeholder scan — PASS
- Focused regression suite: **20 passed, 3 skipped** — PASS for all runnable tests.
- The 3 skipped tests are the real PostgreSQL integration tests because this execution environment has **no PostgreSQL server and no `PHANTA_TEST_DATABASE_URL`**.
- Python syntax/compile checks — PASS.
- Manual secret scan — PASS. The generic scan had false positives on the substring `EA` in normal identifiers/text; inspection found no actual API key/private-key material.
- Clean release source contains no Python cache artifacts.

## PostgreSQL execution limitation
A real PostgreSQL server/connection is required to execute the three integration tests. No PostgreSQL binary/server and no database URL are available in this environment. Therefore it would be false to claim that the RLS/webhook integration tests were run against PostgreSQL.

Run on the dedicated test database with:

```powershell
$env:PHANTA_TEST_DATABASE_URL="postgresql://USER:PASSWORD@HOST:5432/TEST_DB"
python -m pytest -q tests/integration_postgres_rls_webhooks.py
```

The test database must already be migrated to Alembic head `0012_platform_admin_read_policy`.

## Final status
**Code changes complete. PostgreSQL integration execution remains pending because no PostgreSQL test database is available in this environment.**

Do not treat the PostgreSQL tests as passed until the command above reports `4 passed` (or the exact number of collected tests if the test file changes).
