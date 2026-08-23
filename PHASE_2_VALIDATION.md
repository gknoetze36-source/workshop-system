# PHANTA Phase 2 Validation Report

## Implemented

- Complete relational Phase 2 models.
- Explicit tenant ownership on tenant-owned data.
- Tenant guard enforcing cross-record ownership before repository writes.
- Tenant-scoped repositories for customers, vehicles, bookings, services,
  conversations, messages, recommendations, quotes, follow-ups, tasks,
  summaries, tool executions and audit logs.
- Customer correction and soft-deletion workflow with audit records.
- Alembic migration for the full schema.
- PostgreSQL forced Row-Level Security.
- PostgreSQL booking overlap EXCLUDE constraints for bays and technicians.
- PostgreSQL append-only approval trigger.
- Transaction-scoped `app.tenant_id` helper with fail-closed behavior.
- PostgreSQL validation script for the actual Railway database.
- SQLite migration smoke test.
- Unit tests for relationships, tenant ownership, approval history,
  conversation/tool history, audit logs, tenant guard and data lifecycle.

## Local automated result

`PYTHONPATH=. pytest -q`

**8 passed, 3 skipped**

The skipped tests are PostgreSQL-only and run automatically when
`PHANTA_TEST_POSTGRES_URL` or `DATABASE_URL` points to PostgreSQL.

`DATABASE_URL=sqlite:////tmp/phanta_phase2.db alembic upgrade head`

**Success**
- Revision: `0001_phase2_foundation`
- Tables created: 32

## Production gate still requiring the real Railway database

Run:

`PHANTA_TEST_POSTGRES_URL="$DATABASE_URL" python scripts/validate_phase2_postgres.py`

The script intentionally fails if the connected role is a PostgreSQL superuser,
because superusers bypass RLS and therefore cannot prove tenant isolation.

It verifies:
1. forced RLS exists,
2. tenant B cannot read tenant A data,
3. overlapping active bookings are rejected by PostgreSQL,
4. approval updates are rejected by the append-only trigger.

A separate backup/restore drill must be performed against the Railway database
before Phase 2 is signed off. This cannot be honestly simulated from the codebase.
