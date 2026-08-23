# PHANTA Database Migration Plan

Phase 2 is implemented with SQLAlchemy models in `models/core.py` and
`models/integration_models.py`.

## Local development
Use `database.init_db()` for a quick SQLite smoke test.

## Production
Use Alembic migrations against PostgreSQL. The production migration set must
also add PostgreSQL-only controls that cannot be expressed portably in the
model layer:

1. Row-Level Security (RLS) policies for tenant-owned tables are included in `0001_phase2_foundation.py`.
2. PostgreSQL EXCLUDE constraints preventing overlapping active bookings for
   the same bay or technician are included in the migration.
3. Approvals are protected by a PostgreSQL append-only trigger.
4. Any provider-specific indexes/partial indexes justified by query plans.

## Required schema invariants
- Every tenant-owned record has a tenant ownership path.
- Customer WhatsApp number is unique within a tenant.
- External webhook identifiers are idempotency keys.
- Approval records are append-only.
- Quote totals are derived from persisted line items and validated server-side.
- Sensitive integration tokens are references to secrets, not plaintext tokens.

The application-level overlap check exists for fast feedback; the PostgreSQL
EXCLUDE constraint is the concurrency-safe final guarantee.


## Runtime tenant context
Before querying or writing tenant-owned data in a PostgreSQL transaction, the
application must call:

```python
set_tenant_id(session, tenant_id)
```

The helper uses transaction-local `set_config('app.tenant_id', ...)`. If the
tenant context is absent, the RLS policy does not match tenant rows. This is
deliberate fail-closed behavior.

## PostgreSQL-only verification still outstanding
The development environment used for this package does not provide a running
PostgreSQL server, so the RLS policies and GiST EXCLUDE constraints were
validated structurally in the migration but must be exercised against the
actual Railway PostgreSQL database before production.
