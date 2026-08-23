# PHANTA Legacy Code Cleanup — Forensic Pass

## Scope
Source: `PHANTA_LEGACY_CLEANUP_STEP(1).zip`.
Target runtime architecture: Owner -> Location. No franchise/branch business ownership.

## Safe removals performed

### E — DEAD CODE
1. `ai/providers/openai_client.py`
   - It only aliased `OpenAIProvider` as `OpenAIClient`.
   - Repository-wide search found no internal importer/caller of `OpenAIClient` or this module.
   - Canonical provider is under `integrations/ai/providers/openai_provider.py`.
   - Removed.

2. `services/financial_service.py:get_payments_for_faction`
   - Repository-wide search found no caller.
   - It was only a misspelled backward-compatible alias forwarding to the location-based function.
   - Removed.

3. `helpers/common.py:user_scope_clause`
   - Repository-wide search found no caller.
   - Removed.

4. `helpers/common.py:role_label`
   - Repository-wide search found no caller.
   - Removed.

5. `helpers/common.py:plan_label`
   - Repository-wide search found no caller.
   - Removed.

### F — DUPLICATE/FACADE CONSOLIDATION
6. `platform_helpers.py`
   - It contained only re-exports of `DONE_STATUSES`, `ROLE_LABELS`, and `available_roles_for_creator`.
   - Internal callers were:
     - `phanta_app.py`
     - `repositories/permission_repository.py`
   - Those imports were changed to the canonical modules:
     - `constants.booking_constants`
     - `helpers.permission`
     - `helpers.common`
   - The facade was then removed.
   - No external/internal repository caller remained.

## Intentionally NOT removed

### B — ACTIVE DEPLOYMENT/OPERATIONS
- `database/schema.py`
- `database/initialize.py`
- `database/predeploy.py`
- `database/compatibility.py`
- `database/indexes.py`

Evidence: these are directly imported/called from the active database initialization and Railway pre-deploy paths. They cannot be classified as dead code from repository evidence alone.

`database/schema.py` still contains legacy physical-schema concepts such as `workshops`, `workshop_id`, `messaging_accounts`, `whatsapp_numbers`, and `legacy_source_key`. Because the active pre-deploy path invokes `_create_tables()` and `_ensure_columns()`, removing these structures now would be a database/deployment migration, not safe code cleanup. They require a separate production database reconciliation/migration task.

### C — REQUIRED MIGRATION HISTORY
Alembic migration files containing historical `tenant_id`, `franchise_id`, or `branch_id` references were retained. They are migration history and must not be rewritten/deleted merely because the active architecture changed.

### G / REVIEW REQUIRED
Documentation and historical reports containing old terminology were not mass-deleted because the task explicitly requires evidence-based removal and because they may document migration history.

## Legacy terminology after cleanup

Textual occurrences remain. This is intentional.

- `franchise` / `franchise_id`: remaining references are in documentation, tests that validate the migration, and historical Alembic migration `0016_owner_location_foundation.py`; no active business ownership implementation was found.
- `branch` / `branch_id`: remaining references are in documentation/tests and historical Alembic migrations; no active business ownership implementation was found.
- `tenant` / `tenant_id`: remaining references are predominantly documentation and historical migrations. The active application uses `location_id` for business scope.
- `workshop_id`: remains in active `database/schema.py` and `database/indexes.py`, which are part of the active legacy/raw schema bootstrap. This is the main unresolved runtime legacy area and should NOT be deleted blindly.
- `messaging_provider.py` explicitly states it no longer reads the legacy `messaging_accounts/workshop_id` hierarchy.

## Tests

### PASS
- Python compileall: PASS.
- `tests/unit/test_owner_location_foundation.py`: 3 passed.
- `tests/unit/test_step4_location_industry_separation.py`: 3 passed.

### FAIL / UNVERIFIED
- Full pytest collection cannot complete because `tests/integration/test_owner_location_isolation_railway.py` deliberately exits when `DATABASE_URL` is not PostgreSQL. No Railway PostgreSQL credentials were available in this environment.
- `tests/unit/test_service_rules.py`: 5 failed because test fixtures attempt to create a `Location` without the now-required `owner_id`. This exposes an existing test/schema alignment problem; it was not hidden or marked PASS.
- Direct Flask import could not be executed because this analysis environment does not have the repository's Flask dependency installed. `requirements.txt` declares Flask, but the current sandbox environment lacks it.

## Important finding

The repository is NOT yet at “zero unnecessary legacy runtime code.”

The proven-dead code above is removed, but the database bootstrap path still contains an active legacy/raw schema compatibility layer. That layer is materially connected to Railway pre-deploy and therefore needs a dedicated migration/schema-authority decision before removal.

## Final active architecture

OWNER
  |
  v
LOCATION
  ├── Customers
  ├── Vehicles
  ├── Bookings
  ├── WhatsApp
  ├── Automations
  ├── Services
  └── Industry Configuration

## Recommended next task

Do NOT continue deleting files by keyword.

The next safe task is:

**DATABASE SCHEMA AUTHORITY + LEGACY BOOTSTRAP RECONCILIATION**

Compare the actual Railway PostgreSQL schema with Alembic and the active raw bootstrap (`database/schema.py`, `database/initialize.py`, `database/compatibility.py`, `database/predeploy.py`). Only after that comparison can the remaining legacy bootstrap code be safely removed or reduced.
